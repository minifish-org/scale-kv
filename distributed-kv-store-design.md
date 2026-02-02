# 分布式 KV 存储 - TDD 设计文档

## 一、TDD 流程概述

```
┌─────────────────────────────────────────────────────────────────┐
│                        TDD 循环                                  │
│                                                                 │
│   1. RED    → 写一个失败的测试                                   │
│   2. GREEN  → 写最少代码让测试通过                               │
│   3. REFACTOR → 重构代码                                        │
│   4. 重复   → 下一个测试                                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 二、测试金字塔

```
                    YCSB 基准测试
                  /              \
           集成测试              冒烟测试
          /        \            /     \
       组件交互    网络通信    快速验证  核心路径
```

## 三、测试分层

| 层级 | 文件 | 目的 | 运行频率 |
|------|------|------|----------|
| **单元测试** | `src/**/*.rs` | 核心逻辑 | 每次提交 |
| **集成测试** | `tests/**/*.rs` | 组件交互 | 每次提交 |
| **YCSB 基准** | `benches/**/*.rs` | 性能验证 | 定期 |
| **冒烟测试** | `tests/smoke.rs` | 快速验证 | 每次 PR |

---

## 四、网络开销与使用准则

基于当前架构和基准测试，得到以下明确结论：

1. **网络开销远高于本地访问**，单条 RPC 写入成本极高。
2. **大粒度批量操作能显著摊薄慢介质成本**，网络和磁盘同理。
3. **读应尽量本地缓存命中，写应使用大批量接口**（如 16K batch），这是正确使用方式。

---

## 五、存储与索引选择（当前决策）

### 5.1 Compute 端（单实例）
- 选择：**自研 B+tree + 全树 RwLock（并发读、写串行）**
- Page 管理：**HashMap + RwLock**
- Free Space Map：**Vec<VecDeque<PageId>> 分桶**（单写线程访问）
- 状态：**已完成**（B+tree 入口、页缓存、FSM、Slotted Page + defrag）

#### 5.1.1 计算层页式模型（决定）
- KV 对应 Page：**一个 Page 可容纳多个 KV**（slotted page）
- B+tree：**key -> (page_id, slot_id)**
- Value：**写入 page payload（key/value 记录）**
- 说明：页内碎片在写入失败且空间足够时触发 defrag

### 5.2 Storage 端（批量写为主）
- 选择：**Bitcask 风格**（多文件 append‑only）
- 内存索引：**HashMap**，存储 `key -> (file_id, offset, len, checksum)`
- 备注：后台 compaction 分段执行，避免阻塞前台写入
- 状态：**已完成**（append‑only 段文件、内存索引、段滚动、compaction、manifest + fsync、自动触发策略）

### 5.3 远端 WAL 批量写（设计草案）
- 目标：**用 WAL 批量代替整页写入**，减少 RPC 开销
- 批量大小：**256KB 固定**（不做超时 flush）
- 发送策略：**前台不阻塞**，WAL 只入本地队列；后台异步攒批发送并等待 ACK
- 触发条件：后台线程仅在 **buffer 达到 256KB** 时发送（不做超时 flush）
- RPC：**新增 appendWal(batch)**（专用 WAL 追加接口）
- Storage：**WAL 顺序落盘 + segment（A+B：顺序追加 + 分段轮换）**
- ACK 语义：**Storage 收到 WAL batch 并成功入队后立即 ACK**（不等落盘/回放）
- Replay：**后台异步回放 WAL → page → Bitcask**（按 page_id 聚合，阈值 8KB）
- 读取：**只从 Compute 读**（Storage 仅用于持久化与压缩）
- 崩溃恢复：**启动时扫描 WAL segment，从 wal_state.last_applied_lsn 继续回放**

#### 5.3.1 WAL 记录最小格式（KV redo）
- 记录粒度：KV 级别
- 字段：`lsn | op | page_id | slot_id | key_len | val_len | key | value`
- 备注：`txn_id/checksum` 可选

#### 5.3.2 写入流程（前台不阻塞）
```mermaid
flowchart TD
    A[Compute put/delete] --> B[append to in-memory WAL buffer]
    B --> C{buffer size < 256KB?}
    C -->|yes| D[return immediately]
    C -->|no| E[enqueue WAL batch]
    E --> F[appendWal RPC (256KB)]
    F --> G[Storage enqueue]
    G --> H[ack]
```

#### 5.3.3 Storage 语义（更新：持久 WAL + 早 ACK）
- `appendWal` 成功 = **已入队（收到即 ACK）**，不代表 durability
- WAL **顺序 append + segment**（A+B 方案）
- WAL **落盘后后台 replay**，不阻塞写入吞吐
- compaction 保留最新值，旧版本清理

#### 5.3.4 KV + page/slot redo（当前选择：KV redo）
- 网络传输：**用户 KV + page_id + slot_id + lsn**
- Storage：按 lsn **回放到 page**，再以 page 为单位写入 Bitcask
- 作用：网络只发增量，但 Storage 仍维护与 Compute 一致的 page

最小记录格式建议：
```
record {
  lsn: u64
  op: PUT | DEL
  page_id: u64
  slot_id: u16
  key_len: u32
  val_len: u32
  key: bytes
  value: bytes
}
```

关键约束：
- **page_id/slot_id 由 Compute 单写者生成**（必须全局唯一/有序）
- **LSN 由 Compute 单写者生成**（严格递增）
- Storage 按 **lsn 顺序重放**，需要幂等处理（`lsn > last_applied` 才 apply）

#### 5.3.5 WAL 落盘与 replay（A+B 方案）
- **WAL 落盘**：Storage 端顺序 append + segment（如 64MB）
- **ACK 语义**：Storage 收到 WAL batch 并成功入队后立即 ACK
- **Replay 线程**：后台读取 WAL，按 lsn 顺序回放更新内存 page，按 `page_id` 聚合后写 Bitcask
- **聚合阈值**：每 page **8KB** 写回触发（不使用时间触发）
- **LSN**：严格递增，`lsn > last_applied` 才 apply
- **wal_state**：回放完成后持久化 `last_applied_lsn`

### 5.6 滑动窗口攒批方案（2026-02-01）
- **目标**：降低平均延迟，避免 Group Commit 的"等待攒批"问题
- **核心洞察**：不是"等攒够再发"，而是"流水线化"

**原理对比**：

| 方案 | 发送时机 | 平均延迟 |
|------|---------|---------|
| 传统 Group Commit | 攒满 256KB 才发 | 可能等几十 ms |
| 滑动窗口 | 窗口有空就发 | ≈ 1 个 RTT |

**滑动窗口工作方式**：
```
控制参数：窗口大小 N（例如 16 或 32）

时间线示例（窗口=4）：
T1: 发送 Req1 Req2 Req3 Req4 （窗口满）
T2: Req1 完成 → 发送 Req5
T3: Req2 完成 → 发送 Req6
T4: Req3 完成 → 发送 Req7
...
```

**伪代码**：
```rust
struct BatchSender {
    in_flight: Arc<AtomicUsize>,
    max_in_flight: usize,
    pending: Mutex<Vec<Request>>,
    sender: Sender<Request>,
}

impl BatchSender {
    async fn send(&self, req: Request) {
        let current = self.in_flight.fetch_add(1, Ordering::AcqRel);

        if current < self.max_in_flight {
            // 窗口有空，直接发送
            self.send_direct(req).await;
        } else {
            // 窗口满，攒到队列
            let mut pending = self.pending.lock().unwrap();
            pending.push(req);
        }
    }

    async fn on_complete(&self) {
        self.in_flight.fetch_sub(1, Ordering::Release);

        // 检查是否有等待的请求
        let req = {
            let mut pending = self.pending.lock().unwrap();
            pending.pop()
        };
        if let Some(req) = req {
            self.send_direct(req).await;
        }
    }
}
```

**Cap'n RPC 配合**：
- Cap'n RPC 支持 Promise/流水线
- 需要应用层实现窗口控制
- 攒批大小可动态调整

**调优参数**：
- 窗口大小：16-64 之间，根据 RTT 调整
- 最佳实践：窗口 × 单请求大小 ≈ 1-2 个 RTT 能发送的数据量

**预期效果**：
```
假设：RTT = 0.5ms，单请求 RPC 开销 = 0.1ms

滑动窗口（窗口=16）：
  - 平均延迟 ≈ 1 个 RTT = 0.5ms
  - 吞吐量 = 窗口 / RTT = 16 / 0.5ms = 32K QPS
```

### 5.7 段页式存储重构（PostgreSQL 风格）

#### 5.7.1 问题分析：Bitcask 不适合 Page 存储

当前 Storage 端使用 Bitcask 风格存储 page：

```
当前架构：
┌──────────────────────────────────────────────────────────────┐
│  Compute                                                      │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  HashMap<PageId, Page>  (原地更新)                      │  │
│  │  B+tree index                                           │  │
│  └────────────────────────────────────────────────────────┘  │
│                         │ WAL batch (KV redo)                 │
│                         ▼                                     │
└──────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────┐
│  Storage (Bitcask)                                            │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  HashMap<PageId, IndexEntry>  (内存索引)                │  │
│  │  segment-*.log (append-only)                            │  │
│  │  每次 page 更新 → 追加完整 16KB                          │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

**问题**：

| 问题 | 原因 | 影响 |
|------|------|------|
| **空间放大** | 每次更新追加完整 page | 1 个 page 更新 10 次 = 160KB 磁盘占用 |
| **Compaction 压力** | stale entries 累积 | 后台 compaction 阻塞、抖动 |
| **架构不对称** | Compute 原地更新，Storage 追加 | 复杂度高，难以理解 |
| **恢复依赖扫描** | 启动时扫描所有 segment 重建索引 | 数据量大时启动慢 |

**Bitcask 适合的场景**：
- 小 value（< 1KB）
- 写多读少
- 需要版本历史

**Page 存储的特点**：
- 固定大小（16KB）
- 更新频繁
- 不需要多版本
- 与 Compute 端模型一致

#### 5.7.2 目标：段页式模型（PostgreSQL 风格）

```
改造后架构：
┌──────────────────────────────────────────────────────────────┐
│  Compute                                                      │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  HashMap<PageId, Page>  (原地更新)                      │  │
│  │  B+tree index                                           │  │
│  └────────────────────────────────────────────────────────┘  │
│                         │ WAL batch (KV redo)                 │
│                         ▼                                     │
└──────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────┐
│  Storage (段页式)                                             │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  HashMap<PageId, Page>  (buffer pool, 原地更新)         │  │
│  │  page_file (固定偏移: page_id × PAGE_SIZE)              │  │
│  │  WAL segments (崩溃恢复用，复用现有实现)                  │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

**核心改变**：
1. **去掉 Bitcask**：不再使用 append-only segment + 内存索引
2. **固定偏移页文件**：`page_file[page_id × 16KB]`，原地覆盖写
3. **Buffer Pool**：内存缓存 dirty pages，批量刷盘
4. **WAL 复用**：现有 WAL 机制不变，用于崩溃恢复

#### 5.7.3 数据结构设计

```rust
/// 段页式存储节点
pub struct PageStore {
    dir: PathBuf,
    
    /// Buffer pool: 内存中的 pages
    /// - 读取时先查 buffer，miss 则从磁盘加载
    /// - 写入时更新 buffer，标记 dirty
    buffer_pool: RwLock<HashMap<PageId, BufferPage>>,
    
    /// 页文件: 固定偏移，page_id × PAGE_SIZE
    /// - 单文件，支持稀疏文件（未写入的 page 不占磁盘空间）
    /// - 或多文件分段（每 1GB 一个文件）
    page_file: Mutex<PageFile>,
    
    /// WAL writer: 复用现有实现
    wal_sender: Mutex<Option<Sender<WalBatch>>>,
    
    /// Checkpoint 状态
    checkpoint_lsn: AtomicU64,
    
    /// 已分配的最大 page_id（用于分配新 page）
    max_page_id: AtomicU64,
}

/// Buffer pool 中的单个 page
struct BufferPage {
    /// 页面数据
    data: Box<[u8; PAGE_SIZE]>,
    
    /// 是否为脏页（需要刷盘）
    dirty: bool,
    
    /// 页面的 LSN（用于 WAL 恢复判断）
    lsn: u64,
}

/// 页文件抽象
struct PageFile {
    /// 单文件模式: 一个大文件
    file: File,
    
    /// 文件当前大小（用于判断是否需要扩展）
    size: u64,
}
```

#### 5.7.4 页文件布局

**方案 A：单文件（简单，推荐先实现）**

```
page_file:
┌─────────────────────────────────────────────────────────────┐
│ Page 0      │ Page 1      │ Page 2      │ ... │ Page N     │
│ [0, 16KB)   │ [16KB, 32KB)│ [32KB, 48KB)│     │            │
└─────────────────────────────────────────────────────────────┘
              │
              └── offset = page_id × PAGE_SIZE

特点：
- 简单直接
- 依赖文件系统稀疏文件支持（Linux ext4/xfs, macOS APFS）
- page_id 不连续时，中间空洞不占磁盘空间
```

**方案 B：分段文件（可选，大数据量时）**

```
data/
├── pages-0000000000.dat    # page_id 0 ~ 65535
├── pages-0000000001.dat    # page_id 65536 ~ 131071
└── pages-0000000002.dat    # ...

每个文件: 65536 × 16KB = 1GB
文件内偏移: (page_id % 65536) × PAGE_SIZE
```

**当前选择**：方案 A（单文件），简单且够用。

#### 5.7.5 读写流程

**读取流程**：

```
get(page_id) -> Option<Page>
    │
    ▼
┌─────────────────────────┐
│ 1. 查 buffer_pool       │
│    RwLock::read()       │
└───────────┬─────────────┘
            │
    ┌───────┴───────┐
    │ hit?          │
    ▼               ▼
  返回 data    ┌─────────────────────────┐
               │ 2. 从 page_file 读取     │
               │    seek(page_id × 16KB) │
               │    read_exact(16KB)     │
               └───────────┬─────────────┘
                           │
                           ▼
               ┌─────────────────────────┐
               │ 3. 插入 buffer_pool     │
               │    dirty = false        │
               └───────────┬─────────────┘
                           │
                           ▼
                        返回 data
```

**写入流程（WAL replay 触发）**：

```
put(page_id, data, lsn)
    │
    ▼
┌─────────────────────────┐
│ 1. 更新 buffer_pool     │
│    RwLock::write()      │
│    dirty = true         │
│    lsn = lsn            │
└───────────┬─────────────┘
            │
            ▼
        返回 Ok(())

注意：
- 写入只更新 buffer，不立即刷盘
- 刷盘由 checkpoint 触发
- WAL 已经持久化，buffer 丢失可恢复
```

**Checkpoint 流程**：

```
checkpoint()
    │
    ▼
┌─────────────────────────────────────────┐
│ 1. 收集所有 dirty pages                  │
│    let dirty_pages = buffer_pool        │
│        .iter()                          │
│        .filter(|p| p.dirty)             │
│        .collect();                      │
└───────────────────┬─────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────┐
│ 2. 按 page_id 排序（顺序写优化）          │
│    dirty_pages.sort_by_key(|p| p.id);   │
└───────────────────┬─────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────┐
│ 3. 批量写入 page_file                    │
│    for page in dirty_pages:             │
│        seek(page_id × PAGE_SIZE)        │
│        write_all(page.data)             │
│        page.dirty = false               │
└───────────────────┬─────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────┐
│ 4. fsync + 更新 checkpoint_lsn          │
│    page_file.sync_all()                 │
│    checkpoint_lsn = max(dirty lsn)      │
│    write_checkpoint_state()             │
└───────────────────┬─────────────────────┘
                    │
                    ▼
                返回 Ok(())
```

#### 5.7.6 崩溃恢复

**恢复流程**：

```
open(dir) -> Result<PageStore>
    │
    ▼
┌─────────────────────────────────────────┐
│ 1. 读取 checkpoint_state                 │
│    checkpoint_lsn = read_checkpoint()   │
└───────────────────┬─────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────┐
│ 2. 打开 page_file                        │
│    max_page_id = file_size / PAGE_SIZE  │
└───────────────────┬─────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────┐
│ 3. 重放 WAL（复用现有逻辑）               │
│    for record in wal where              │
│        record.lsn > checkpoint_lsn:     │
│        apply_record(record)             │
└───────────────────┬─────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────┐
│ 4. 启动后台 WAL replay 线程              │
│    start_wal_replay()                   │
└───────────────────┬─────────────────────┘
                    │
                    ▼
                返回 Ok(store)
```

**关键点**：
- `checkpoint_lsn` 之前的 WAL 可安全删除
- 恢复只需重放 `checkpoint_lsn` 之后的 WAL
- 无需扫描 page_file 重建索引（与 Bitcask 不同）

#### 5.7.7 Checkpoint 策略

**触发条件**（满足任一）：

| 条件 | 阈值 | 说明 |
|------|------|------|
| **Dirty pages 数量** | > 1000 | 避免 buffer 过大 |
| **Dirty bytes 总量** | > 64MB | 控制内存使用 |
| **时间间隔** | > 60s | 定期刷盘 |
| **WAL 大小** | > 256MB | 允许清理旧 WAL |

**实现**：

```rust
impl PageStore {
    fn maybe_checkpoint(&self) {
        let stats = self.buffer_stats();
        
        let should_checkpoint = 
            stats.dirty_count > 1000 ||
            stats.dirty_bytes > 64 * 1024 * 1024 ||
            stats.since_last_checkpoint > Duration::from_secs(60);
        
        if should_checkpoint {
            let _ = self.checkpoint();
        }
    }
}
```

#### 5.7.8 与现有代码的关系

**保留**：
- `WalBatch`, `WalRecord` 结构
- `WalWriter`, `wal_writer_loop` 逻辑
- WAL segment 格式和读写
- `wal_state` 持久化

**移除**：
- Bitcask segment 文件（`segment-*.log`）
- `IndexEntry { file_id, offset }` 内存索引
- `compact()` 和相关逻辑
- `stale_entries` 统计

**修改**：
- `StorageNode` → `PageStore`（或保留名称，替换实现）
- `put(page_id, data)` → 更新 buffer，标记 dirty
- `get(page_id)` → buffer pool 查找 + 磁盘回退
- 崩溃恢复逻辑简化

#### 5.7.9 对比分析

| 方面 | Bitcask (当前) | 段页式 (改造后) |
|------|---------------|----------------|
| **更新开销** | 追加 16KB | 原地覆盖 16KB |
| **空间放大** | 高（多版本累积） | 无（1:1） |
| **Compaction** | 必须，定期执行 | 不需要 |
| **启动恢复** | 扫描所有 segment | 只重放 WAL 增量 |
| **内存索引** | `HashMap<PageId, IndexEntry>` | 无需（固定偏移计算） |
| **代码复杂度** | 高（compaction, manifest） | 低 |
| **与 Compute 对称** | 否 | 是 |

#### 5.7.10 实现步骤

1. **Phase 1: PageStore 基础结构**
   - 新建 `PageStore` 结构
   - 实现 `open()`, `get()`, `put()`
   - 单文件 page_file 读写

2. **Phase 2: Buffer Pool**
   - 实现 `BufferPage` 和 dirty 跟踪
   - 实现 `checkpoint()`
   - `checkpoint_state` 持久化

3. **Phase 3: WAL 集成**
   - 复用现有 `WalWriter`
   - 修改 `wal_replay_loop` 调用 `PageStore`
   - 崩溃恢复测试

4. **Phase 4: 迁移**
   - 替换 `StorageNode` 实现
   - 更新 `StorageServer` RPC 处理
   - 清理 Bitcask 相关代码

5. **Phase 5: 测试验证**
   - 现有测试通过
   - 新增 checkpoint 测试
   - 崩溃恢复测试
   - 性能对比

#### 5.7.11 文件布局（改造后）

```
data/
├── pages.dat           # 页文件（固定偏移）
├── checkpoint_state    # checkpoint LSN
├── wal-*.log           # WAL segments（复用）
└── wal_state           # WAL 回放状态（复用）
```

### 5.8 页式 B+tree（索引即 page）方案

- 目标：索引节点本身就是固定 16KB page，root page_id 持久化
- Storage 仅提供 `page_id -> page bytes`（可复用段页式存储）
- Compute 维护 buffer pool（页缓存 + dirty flush）

**页头建议：**
```
PageHeader {
  page_id: u64
  page_type: u8    // INTERNAL=1, LEAF=2
  level: u8        // 0=leaf
  key_count: u16
  free_start: u16
  free_end: u16
  lsn: u64         // redo 顺序
}
```

**Leaf Page：**
- 记录 `(key, value)` 或 `(key, slot_ref)`

**Internal Page：**
- `keys[]` + `child_page_id[]`（数量 = key_count + 1）

**WAL（必须）：**
- page 修改写 redo
- commit 时 WAL durable，再刷脏页

**最小实现路径：**
1) page 格式 + 序列化
2) leaf‑only B+tree（无 internal）
3) internal + split
4) buffer pool + dirty flush
5) WAL redo + recovery

#### 约束
- **存储为单写者模型**，写入串行
- 内存索引为 HashMap（无锁），由单写者线程更新

---

### 5.9 代码审查结果（2026-02-01）

#### 5.9.1 严重问题

| 严重程度 | 问题 | 位置 | 影响 |
|---------|------|------|------|
| 🔴 | Cap'n RPC 同步使用 | client.rs:287 | RPC 开销高，延迟大 |
| 🔴 | B+Tree 无叶子链表 | bptree.rs:10-18 | Range scan 效率低 |
| 🔴 | 同步文件 I/O 阻塞 async | node.rs:156 | 阻塞 tokio 线程池 |
| 🔴 | `spawn_local` 兼容性 | server.rs:165 | 可能在 multi-thread runtime 中异常 |

#### 5.9.2 详细问题说明

**Cap'n RPC 同步使用**：
```rust
// 当前：每条 RPC 阻塞等待
if let Some(storage) = &self.storage {
    storage.put(page_id, &page).await?;  // ← 逐条等待
}

// 滑动窗口方案（5.3.5）：窗口有空就发，无空就排队
```

**B+Tree 无叶子链表**：
```rust
// 当前：只有 keys
Node::Leaf {
    keys: Vec<K>,
},

// 建议：添加兄弟指针
Node::Leaf {
    keys: Vec<K>,
    prev: Option<Box<Node<K>>>,
    next: Option<Box<Node<K>>>,
},
```

**同步文件 I/O**：
```rust
// 当前：使用 std::fs::File（阻塞）
writer.file.write_all(value)?;

// 建议：改用 tokio 异步 API
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
```

**spawn_local 兼容性**：
```rust
// 当前：混用 spawn_local + features = ["full"]
tokio::task::spawn_local(async move { ... });
// features = ["full"] 包含 rt-multi-thread

// 方案 A：统一用 spawn
tokio::task::spawn(async move { ... });

// 方案 B：用 current_thread runtime
// Cargo.toml: features = ["rt-current-thread", "net"]
```

#### 5.9.3 中等问题

| 严重程度 | 问题 | 位置 | 修复建议 |
|---------|------|------|----------|
| 🟡 | 锁内异步 I/O | client.rs:230-251 | 先释放锁，再做 I/O |
| 🟡 | MAX_KEYS = 8 太小 | bptree.rs:3 | 根据 page 大小动态调整 |
| 🟡 | 批量操作未并发 | client.rs:387 | 用 `join_all` 并发执行 |

#### 5.9.4 轻微问题

| 严重程度 | 问题 | 位置 | 修复建议 |
|---------|------|------|----------|
| 🟢 | 未使用 dashmap | Cargo.toml:11 | 移除依赖 |
| 🟢 | tokio features 过量 | Cargo.toml:14 | 用 `rt-multi-thread + net` |

#### 5.9.5 修复优先级

| 优先级 | 问题 | 预计改动 |
|--------|------|----------|
| **P0** | 同步文件 I/O → 异步 | node.rs 较大改动 |
| **P0** | Cap'n RPC 滑动窗口 | client.rs 中等改动 |
| **P1** | spawn_local 兼容性 | server.rs 小改动 |
| **P2** | B+Tree 叶子链表 | bptree.rs 中等改动 |
| **P3** | 清理依赖 | Cargo.toml |

---

## 四、存储节点 - 单元测试

### 4.1 测试文件结构

```
src/storage/
├── mod.rs
├── node.rs              # 存储节点核心逻辑（DashMap）
├── lock_table.rs        # 锁表实现
└── tests/
    ├── mod.rs
    ├── basic.rs         # 基础读写测试
    ├── concurrent.rs    # 并发测试
    └── lock.rs          # 锁测试
```

### 4.2 基础读写测试

```rust
// src/storage/tests/basic.rs

use crate::storage::StorageNode;

#[test]
fn test_put_and_get() {
    // RED: 先写测试，期望失败
    let node = StorageNode::new();
    
    // GREEN: 实现代码后测试通过
    node.put("foo", b"bar");
    
    let value = node.get("foo");
    assert_eq!(value, Some(b"bar".to_vec()));
}

#[test]
fn test_get_missing_key() {
    let node = StorageNode::new();
    let value = node.get("missing");
    assert_eq!(value, None);
}

#[test]
fn test_overwrite() {
    let node = StorageNode::new();
    node.put("foo", b"bar");
    node.put("foo", b"baz");
    
    let value = node.get("foo");
    assert_eq!(value, Some(b"baz".to_vec()));
}

#[test]
fn test_delete() {
    let node = StorageNode::new();
    node.put("foo", b"bar");
    node.delete("foo");
    
    let value = node.get("foo");
    assert_eq!(value, None);
}

#[test]
fn test_empty_key() {
    let node = StorageNode::new();
    node.put("", b"value");
    
    let value = node.get("");
    assert_eq!(value, Some(b"value".to_vec()));
}

#[test]
fn test_large_value() {
    let node = StorageNode::new();
    let value = vec![0u8; 100_000];
    
    node.put("large", &value);
    let retrieved = node.get("large").unwrap();
    
    assert_eq!(retrieved.len(), 100_000);
}

#[test]
fn test_many_keys() {
    let node = StorageNode::new();
    
    // 插入 10000 个 key
    for i in 0..10_000 {
        node.put(&format!("key_{}", i), &format!("value_{}", i));
    }
    
    assert_eq!(node.len(), 10_000);
    
    // 验证随机读取
    for i in (0..10_000).step_by(1000) {
        let value = node.get(&format!("key_{}", i)).unwrap();
        assert_eq!(value, format!("value_{}").as_bytes());
    }
}
```

### 4.3 并发测试

```rust
// src/storage/tests/concurrent.rs

use std::sync::Arc;
use std::thread;

#[test]
fn test_concurrent_read() {
    let node = Arc::new(StorageNode::new());
    node.put("key", b"value");
    
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let node = node.clone();
            thread::spawn(move || {
                for _ in 0..1000 {
                    let _ = node.get("key");
                }
            })
        })
        .collect();
    
    handles.into_iter().for_each(|h| h.join().unwrap());
}

#[test]
fn test_concurrent_write_different_keys() {
    let node = Arc::new(StorageNode::new());
    
    let handles: Vec<_> = (0..10)
        .map(|i| {
            let node = node.clone();
            thread::spawn(move || {
                for j in 0..1000 {
                    node.put(&format!("key_{}", i * 1000 + j), b"value");
                }
            })
        })
        .collect();
    
    handles.into_iter().for_each(|h| h.join().unwrap());
    
    assert_eq!(node.len(), 10_000);
}

#[test]
fn test_concurrent_put_same_key() {
    let node = Arc::new(StorageNode::new());
    node.put("key", b"initial");
    
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let node = node.clone();
            thread::spawn(move || {
                for i in 0..100 {
                    node.put("key", &format!("value_{}", i).into_bytes());
                }
            })
        })
        .collect();
    
    handles.into_iter().for_each(|h| h.join().unwrap());
    
    // 最终值应该是某次写入的结果
    let value = node.get("key").unwrap();
    assert!(value.starts_with(b"value_"));
}

#[test]
fn test_concurrent_mixed_operations() {
    let node = Arc::new(StorageNode::new());
    
    // 预先插入一些数据
    for i in 0..1000 {
        node.put(&format!("key_{}", i), b"initial");
    }
    
    let mut handles = Vec::new();
    
    // 读线程
    for _ in 0..5 {
        let node = node.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..500 {
                let key = thread_rng().gen_range(0..1000);
                let _ = node.get(&format!("key_{}", key));
            }
        }));
    }
    
    // 写线程
    for i in 0..5 {
        let node = node.clone();
        handles.push(thread::spawn(move || {
            for j in 0..500 {
                node.put(&format!("key_{}", i * 100 + j), b"updated");
            }
        }));
    }
    
    handles.into_iter().for_each(|h| h.join().unwrap());
}
```

### 4.4 锁测试

```rust
// src/storage/tests/lock.rs

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Barrier;

#[test]
fn test_lock_blocks_other_writer() {
    let node = StorageNode::new();
    node.put("key", b"value");
    
    let guard = node.lock("key").unwrap();
    
    // 另一个锁应该失败或阻塞
    let result = node.try_lock("key");
    assert!(result.is_err());
    
    drop(guard);
    
    // 释放后应该能获取锁
    let guard2 = node.lock("key");
    assert!(guard2.is_ok());
}

#[test]
fn test_different_keys_no_contention() {
    let node = StorageNode::new();
    
    let guard1 = node.lock("key1").unwrap();
    let guard2 = node.lock("key2").unwrap();  // 不应该阻塞
    
    drop(guard1);
    drop(guard2);
}

#[test]
fn test_lock_with_operations() {
    let node = StorageNode::new();
    node.put("key", b"old_value");
    
    // 加锁后执行操作
    {
        let _guard = node.lock("key").unwrap();
        node.put("key", b"new_value");
    }
    
    // 解锁后验证
    let value = node.get("key").unwrap();
    assert_eq!(value, b"new_value");
}

#[test]
fn test_concurrent_lock_contention() {
    let node = Arc::new(StorageNode::new());
    let barrier = Arc::new(Barrier::new(10));
    let success_count = Arc::new(AtomicUsize::new(0));
    
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let node = node.clone();
            let barrier = barrier.clone();
            let success = success_count.clone();
            
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..100 {
                    if let Ok(guard) = node.try_lock("contested_key") {
                        node.put("contested_key", b"updated");
                        success.fetch_add(1, Ordering::SeqCst);
                        break;
                    }
                    std::hint::spin_loop();
                }
            })
        })
        .collect();
    
    handles.into_iter().for_each(|h| h.join().unwrap());
    
    // 至少有一个成功获取锁
    assert!(success_count.load(Ordering::SeqCst) > 0);
}
```

---

## 五、计算节点 - 单元测试

### 5.1 测试文件结构

```
src/compute/
├── mod.rs
├── node.rs              # 计算节点核心逻辑
├── client.rs            # 网络客户端
└── tests/
    ├── mod.rs
    ├── basic.rs         # 基础读写测试
    └── cache.rs         # 缓存测试
```

### 5.2 基础读写测试

```rust
// src/compute/tests/basic.rs

use std::net::TcpListener;
use std::thread;
use std::sync::Arc;

use crate::storage::StorageNode;
use crate::compute::ComputeNode;

fn setup_test_infrastructure() -> (Arc<StorageNode>, String) {
    let storage = Arc::new(StorageNode::new());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    
    let storage_clone = storage.clone();
    thread::spawn(move || {
        accept_connection(listener, storage_clone);
    });
    
    (storage, format!("127.0.0.1:{}", addr.port()))
}

#[test]
fn test_compute_put_get() {
    let (_storage, addr) = setup_test_infrastructure();
    let compute = ComputeNode::new(&addr);
    
    compute.put("foo", b"bar").unwrap();
    let value = compute.get("foo").unwrap();
    
    assert_eq!(value, Some(b"bar".to_vec()));
}

#[test]
fn test_compute_update() {
    let (_storage, addr) = setup_test_infrastructure();
    let compute = ComputeNode::new(&addr);
    
    compute.put("key", b"v1").unwrap();
    compute.put("key", b"v2").unwrap();
    
    let value = compute.get("key").unwrap();
    assert_eq!(value, Some(b"v2".to_vec()));
}

#[test]
fn test_compute_missing_key() {
    let (_storage, addr) = setup_test_infrastructure();
    let compute = ComputeNode::new(&addr);
    
    let value = compute.get("missing");
    assert_eq!(value, None);
}

#[test]
fn test_compute_multiple_keys() {
    let (_storage, addr) = setup_test_infrastructure();
    let compute = ComputeNode::new(&addr);
    
    for i in 0..100 {
        compute.put(&format!("key_{}", i), &format!("value_{}", i)).unwrap();
    }
    
    for i in 0..100 {
        let value = compute.get(&format!("key_{}", i)).unwrap();
        assert_eq!(value, Some(format!("value_{}").as_bytes().to_vec()));
    }
}
```

### 5.3 缓存测试

```rust
// src/compute/tests/cache.rs

use std::net::TcpListener;
use std::thread;
use std::sync::Arc;

use crate::storage::StorageNode;
use crate::compute::ComputeNode;

fn setup() -> (Arc<StorageNode>, String) {
    let storage = Arc::new(StorageNode::new());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    
    let storage_clone = storage.clone();
    thread::spawn(move || {
        accept_connection(listener, storage_clone);
    });
    
    (storage, format!("127.0.0.1:{}", addr.port()))
}

#[test]
fn test_cache_hit_miss() {
    let (_storage, addr) = setup();
    let mut compute = ComputeNode::new(&addr);
    
    // 第一次读 - 缓存未命中
    let _ = compute.get("key");
    assert_eq!(compute.cache_misses(), 1);
    assert_eq!(compute.cache_hits(), 0);
    
    // 第二次读 - 缓存命中
    let _ = compute.get("key");
    assert_eq!(compute.cache_hits(), 1);
}

#[test]
fn test_cache_invalidation_on_write() {
    let (_storage, addr) = setup();
    let mut compute = ComputeNode::new(&addr);
    
    // 写入数据
    compute.put("key", b"v1").unwrap();
    
    // 读入缓存
    let _ = compute.get("key");
    assert_eq!(compute.cache_hits(), 1);
    
    // 再次写入 - 应该使缓存失效
    compute.put("key", b"v2").unwrap();
    
    // 下一次读应该重新从存储节点获取
    let _ = compute.get("key");
    assert_eq!(compute.cache_misses(), 2);
}

#[test]
fn test_cache_statistics() {
    let (_storage, addr) = setup();
    let mut compute = ComputeNode::new(&addr);
    
    // 初始状态
    assert_eq!(compute.cache_hits(), 0);
    assert_eq!(compute.cache_misses(), 0);
    assert_eq!(compute.cache_size(), 0);
    
    // 多次读写
    for i in 0..10 {
        compute.put(&format!("key_{}", i), &format!("value_{}", i)).unwrap();
    }
    
    for i in 0..10 {
        let _ = compute.get(&format!("key_{}", i));
    }
    
    // 5 个 key 读两次（命中 5 次）
    for i in 0..5 {
        let _ = compute.get(&format!("key_{}", i));
    }
    
    assert_eq!(compute.cache_hits(), 5);
    assert_eq!(compute.cache_misses(), 10);  // 10 个 key 各 miss 一次
    assert_eq!(compute.cache_size(), 10);
}
```

---

## 六、集成测试

### 6.1 完整工作流测试

```rust
// tests/integration.rs

use std::net::TcpListener;
use std::thread;
use std::sync::Arc;

use crate::storage::StorageNode;
use crate::compute::ComputeNode;

fn setup() -> (Arc<StorageNode>, String) {
    let storage = Arc::new(StorageNode::new());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    
    let storage_clone = storage.clone();
    thread::spawn(move || {
        accept_connection(listener, storage_clone);
    });
    
    (storage, format!("127.0.0.1:{}", addr.port()))
}

#[test]
fn test_full_workflow() {
    let (storage, addr) = setup();
    
    // 创建两个计算节点
    let mut compute1 = ComputeNode::new(&addr);
    let mut compute2 = ComputeNode::new(&addr);
    
    // 计算节点 1 写入
    compute1.put("key1", b"value1").unwrap();
    compute1.put("key2", b"value2").unwrap();
    
    // 计算节点 2 读取
    let v1 = compute2.get("key1").unwrap().unwrap();
    let v2 = compute2.get("key2").unwrap().unwrap();
    
    assert_eq!(v1, b"value1");
    assert_eq!(v2, b"value2");
    
    // 验证存储节点数据
    assert_eq!(storage.get("key1"), Some(b"value1".to_vec()));
    assert_eq!(storage.get("key2"), Some(b"value2".to_vec()));
}

#[test]
fn test_concurrent_computes() {
    let (storage, addr) = setup();
    
    let mut handles = vec![];
    
    for i in 0..4 {
        let addr = addr.clone();
        handles.push(thread::spawn(move || {
            let mut compute = ComputeNode::new(&addr);
            for j in 0..100 {
                compute.put(&format!("key_{}", i * 100 + j), b"value").unwrap();
            }
        }));
    }
    
    handles.into_iter().for_each(|h| h.join().unwrap());
    
    // 验证存储节点数据完整性
    assert_eq!(storage.len(), 400);
}

#[test]
fn test_cross_node_consistency() {
    let (storage, addr) = setup();
    
    let mut compute1 = ComputeNode::new(&addr);
    let mut compute2 = ComputeNode::new(&addr);
    
    // 计算节点 1 写入
    compute1.put("shared", b"from_compute1").unwrap();
    
    // 计算节点 2 读取
    let value = compute2.get("shared").unwrap().unwrap();
    assert_eq!(value, b"from_compute1");
    
    // 计算节点 2 更新
    compute2.put("shared", b"from_compute2").unwrap();
    
    // 计算节点 1 读取最新值
    let value = compute1.get("shared").unwrap().unwrap();
    assert_eq!(value, b"from_compute2");
}
```

---

## 七、YCSB 基准测试

### 7.1 YCSB 工作负载定义

```rust
// benches/ycsb.rs

use kv_store::{ComputeNode, StorageNode};
use rand::Rng;

#[derive(Clone, Copy)]
enum Workload {
    A,  // 50% read, 50% write
    B,  // 95% read, 5% write
    C,  // 100% read
}

struct YcsbClient {
    node: ComputeNode,
    keys: Vec<String>,
    rng: rand::ThreadRng,
    workload: Workload,
}

impl YcsbClient {
    fn new(node: ComputeNode, record_count: usize, workload: Workload) -> Self {
        let keys: Vec<String> = (0..record_count)
            .map(|i| format!("user{:06}", i))
            .collect();
        
        Self {
            node,
            keys,
            rng: rand::thread_rng(),
            workload,
        }
    }
    
    fn run_one_op(&mut self) {
        match self.workload {
            Workload::A => {
                if self.rng.gen::<f64>() < 0.5 {
                    self.do_read();
                } else {
                    self.do_write();
                }
            }
            Workload::B => {
                if self.rng.gen::<f64>() < 0.95 {
                    self.do_read();
                } else {
                    self.do_write();
                }
            }
            Workload::C => {
                self.do_read();
            }
        }
    }
    
    fn do_read(&mut self) {
        let key = self.pick_random_key();
        let _ = self.node.get(&key);
    }
    
    fn do_write(&mut self) {
        let key = self.pick_random_key();
        let value = format!("value_{}", self.rng.gen::<u64>());
        let _ = self.node.put(&key, value.as_bytes());
    }
    
    fn pick_random_key(&mut self) -> &str {
        let idx = self.rng.gen_range(0..self.keys.len());
        &self.keys[idx]
    }
}

fn run_ycsb_workload(ops: usize, threads: usize, workload: Workload) {
    let storage = Arc::new(StorageNode::new());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    
    let storage_clone = storage.clone();
    std::thread::spawn(move || {
        accept_connection(listener, storage_clone);
    });
    
    let start = std::time::Instant::now();
    let mut handles = vec![];
    let ops_per_thread = ops / threads;
    
    for _ in 0..threads {
        let addr = format!("127.0.0.1:{}", addr.port());
        let node = ComputeNode::new(&addr);
        let mut client = YcsbClient::new(node, 100_000, workload);
        handles.push(std::thread::spawn(move || {
            for _ in 0..ops_per_thread {
                client.run_one_op();
            }
        }));
    }
    
    handles.into_iter().for_each(|h| h.join().unwrap());
    
    let duration = start.elapsed();
    let throughput = ops as f64 / duration.as_secs();
    
    println!("=== YCSB {:?} ===", workload);
    println!("Operations: {}", ops);
    println!("Threads: {}", threads);
    println!("Duration: {:?}", duration);
    println!("Throughput: {:.2} ops/sec", throughput);
}
```

### 7.2 YCSB 基准测试定义

```rust
// benches/ycsb.rs (使用 criterion)

use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("ycsb_a_10000", |b| {
        b.iter(|| {
            run_ycsb_workload(10000, 4, Workload::A);
        });
    });
    
    c.bench_function("ycsb_b_10000", |b| {
        b.iter(|| {
            run_ycsb_workload(10000, 4, Workload::B);
        });
    });
    
    c.bench_function("ycsb_c_10000", |b| {
        b.iter(|| {
            run_ycsb_workload(10000, 4, Workload::C);
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
```

---

## 八、冒烟测试

```rust
// tests/smoke.rs

use std::net::TcpListener;
use std::thread;
use std::sync::Arc;

use crate::storage::StorageNode;
use crate::compute::ComputeNode;

fn setup() -> (Arc<StorageNode>, String) {
    let storage = Arc::new(StorageNode::new());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    
    let storage_clone = storage.clone();
    thread::spawn(move || {
        accept_connection(listener, storage_clone);
    });
    
    (storage, format!("127.0.0.1:{}", addr.port()))
}

/// 快速冒烟测试，每个 PR 必须通过
#[test]
fn test_smoke() {
    let (_storage, addr) = setup();
    let mut compute = ComputeNode::new(&addr);
    
    // 基础操作
    compute.put("key1", b"value1").unwrap();
    assert_eq!(compute.get("key1").unwrap(), Some(b"value1".to_vec()));
    
    compute", b"value2").unwrap();
.put("key1    assert_eq!(compute.get("key1").unwrap(), Some(b"value2".to_vec()));
    
    compute.put("key2", b"value2").unwrap();
    assert_eq!(compute.get("key2").unwrap(), Some(b"value2".to_vec()));
    
    println!("Smoke test passed!");
}
```

---

## 九、运行测试

```bash
# 运行所有测试
cargo test

# 运行单元测试
cargo test --lib

# 运行集成测试
cargo test --test integration

# 运行 YCSB 基准测试
cargo bench ycsb

# 运行冒烟测试
cargo test --test smoke

# 带日志运行
RUST_LOG=debug cargo test

# 并发测试
cargo test concurrent --release

# 运行特定测试
cargo test test_put_and_get
cargo test test_compute_put_get
```

---

## 十、测试覆盖率

```bash
# 安装 tarpaulin
cargo install cargo-tarpaulin

# 运行覆盖率
cargo tarpaulin --out Html

# 查看覆盖率报告
open tarpaulin-report.html
```

---

## 十一、TDD 步骤

### 步骤 1：写存储节点单元测试

```rust
// tests/storage_basic_test.rs (RED - 期望失败)
#[test]
fn test_storage_put_get() {
    let node = StorageNode::new();
    node.put("foo", b"bar");
    assert_eq!(node.get("foo"), Some(b"bar".to_vec()));
}
```

### 步骤 2：实现存储节点

```rust
// src/storage/node.rs (GREEN - 通过测试)
use dashmap::DashMap;

pub struct StorageNode {
    data: DashMap<String, Vec<u8>>,
}

impl StorageNode {
    pub fn new() -> Self {
        Self {
            data: DashMap::new(),
        }
    }
    
    pub fn put(&self, key: &str, value: &[u8]) {
        self.data.insert(key.to_string(), value.to_vec());
    }
    
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.data.get(key).map(|v| v.clone())
    }
    
    pub fn len(&self) -> usize {
        self.data.len()
    }
}
```

### 步骤 3：重构和添加更多测试

```rust
// 添加更多测试...
// 重构代码...
```

---

## 十二、总结

| 测试类型 | 位置 | 目的 | 优先级 |
|----------|------|------|--------|
| **单元测试** | `src/**/*.rs` | 核心逻辑 | ⭐⭐⭐ |
| **集成测试** | `tests/**/*.rs` | 组件交互 | ⭐⭐⭐ |
| **YCSB** | `benches/ycsb.rs` | 性能基准 | ⭐⭐ |
| **冒烟** | `tests/smoke.rs` | 快速验证 | ⭐⭐⭐ |

**TDD 流程**：先写测试 → 实现代码 → 重构 → 重复
