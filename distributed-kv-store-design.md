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
- 发送策略：**batch 未满时不阻塞写；满 256KB 时触发一次 RPC 并阻塞等待 ack**
- RPC：**复用 batchPut**（语义变为“追加 WAL 记录”）
- 存储：**仅追加日志 + compaction**（Bitcask），暂不要求回放到 page
- 读取：**只从 Compute 读**（Storage 仅用于持久化与压缩）
- 崩溃恢复：**后续从 Storage 全量扫描重建**（暂不实现）

#### 5.3.1 WAL 记录最小格式
- 记录粒度：KV 级别
- 字段：`key_len | val_len | key | value`
- 备注：可预留 `txn_id/lsn/checksum` 便于未来扩展

#### 5.3.2 写入流程（阻塞仅在 batch 满时）
```mermaid
flowchart TD
    A[Compute put/delete] --> B[append to in-memory WAL buffer]
    B --> C{buffer size < 256KB?}
    C -->|yes| D[return immediately]
    C -->|no| E[batchPut RPC (256KB)]
    E --> F[Storage append log]
    F --> G[ack]
    G --> H[unblock writers]
```

#### 5.3.3 Storage 语义（最小要求）
- `batchPut` 成功 = **日志已追加**（不要求 fsync）
- compaction 保留最新值，旧版本清理

#### 5.3.4 可选方案：KV + page/slot redo（让 page 层真正有用）
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
- Storage 按 **lsn 顺序重放**，需要幂等处理（避免重试重复 apply）

#### 约束
- **存储为单写者模型**（append‑only log），写入串行
- 内存索引为 HashMap（无锁），由单写者线程更新

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
