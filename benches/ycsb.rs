use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::RngCore;
use rand::rngs::StdRng;
use rand::SeedableRng;
use scale_kv::{ComputeNode, StorageServer};
use scale_kv::page_bptree::{InMemoryPageProvider, PageBPlusTree, SlotRef as BptreeSlotRef};
use sled::Config;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::env;
use tokio::runtime::Builder;
use tokio::task::LocalSet;

const NUM_RECORDS: usize = 10_000;
const OPERATIONS: usize = 1_000;
const VALUE_SIZE: usize = scale_kv::VALUE_SIZE;
const KEY_SIZE: usize = scale_kv::KEY_SIZE;
const BATCH_SIZE: usize = 16_384;
const SCAN_LENGTH: usize = 10;

struct ZipfianGenerator {
    items: u64,
    alpha: f64,
}

impl ZipfianGenerator {
    fn new(items: u64) -> Self {
        Self { items, alpha: 0.5 }
    }

    fn next(&mut self, rng: &mut impl RngCore) -> u64 {
        let mut buf = [0u8; 8];
        rng.fill_bytes(&mut buf);
        let u: f64 = f64::from_le_bytes(buf).abs() % 1.0;
        let rank = (self.items as f64) * u.powf(-self.alpha);
        rank.min(self.items as f64 - 1.0) as u64
    }
}

fn key_for(id: u64) -> String {
    key_for_prefix("user", id)
}

fn key_for_prefix(prefix: &str, id: u64) -> String {
    let width = KEY_SIZE.saturating_sub(prefix.len());
    format!("{prefix}{id:0width$}", width = width)
}

fn latest_key_from_rank(rank: u64) -> String {
    let max = NUM_RECORDS.saturating_sub(1);
    let key_num = max.saturating_sub((rank as usize).min(max));
    key_for(key_num as u64)
}

fn setup(rt: &tokio::runtime::Runtime, local: &LocalSet, workers: usize) -> Arc<ComputeNode> {
    local.block_on(rt, async {
        let compute = if in_memory_compute() {
            Arc::new(ComputeNode::new())
        } else {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let server = StorageServer::start(addr).await.unwrap();
            Arc::new(
                ComputeNode::with_storage_workers(&server.addr().to_string(), workers, local)
                    .await
                    .unwrap(),
            )
        };
        let value = vec![0u8; VALUE_SIZE];
        for i in 0..NUM_RECORDS {
            let key = key_for(i as u64);
            compute.put(&key, &value).await.unwrap();
        }
        compute
    })
}

fn setup_sled() -> sled::Db {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Config::new().path(dir.path()).open().unwrap();
    let value = vec![0u8; VALUE_SIZE];
    for i in 0..NUM_RECORDS {
        let key = key_for(i as u64);
        let _ = db.insert(key.as_bytes(), value.as_slice());
    }
    db
}

fn setup_bptree() -> PageBPlusTree<InMemoryPageProvider> {
    let mut tree = PageBPlusTree::new();
    for i in 0..NUM_RECORDS {
        let key = key_for(i as u64);
        let slot_ref = BptreeSlotRef {
            page_id: i as u64,
            slot_id: 0,
        };
        tree.insert(key.into_bytes(), slot_ref).unwrap();
    }
    tree
}

fn setup_bptree_u64() -> PageBPlusTree<InMemoryPageProvider> {
    let mut tree = PageBPlusTree::new();
    for i in 0..NUM_RECORDS {
        let key = u64_key_bytes(i as u64).to_vec();
        let slot_ref = BptreeSlotRef {
            page_id: i as u64,
            slot_id: 0,
        };
        tree.insert(key, slot_ref).unwrap();
    }
    tree
}

fn setup_hashmap_u64() -> HashMap<u64, u64> {
    let mut map = HashMap::with_capacity(NUM_RECORDS);
    for i in 0..NUM_RECORDS {
        map.insert(i as u64, i as u64);
    }
    map
}

fn pregen_zipf_ranks() -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(0x5ca1e_u64);
    let mut zipf = ZipfianGenerator::new(NUM_RECORDS as u64);
    let mut out = Vec::with_capacity(OPERATIONS);
    for _ in 0..OPERATIONS {
        out.push(zipf.next(&mut rng));
    }
    out
}

fn pregen_keys_str_from_ranks(ranks: &[u64]) -> Vec<String> {
    ranks.iter().map(|&rank| key_for(rank)).collect()
}

fn pregen_latest_keys_str_from_ranks(ranks: &[u64]) -> Vec<String> {
    ranks.iter().map(|&rank| latest_key_from_rank(rank)).collect()
}

fn pregen_range_keys_str_from_ranks(ranks: &[u64]) -> Vec<(String, String)> {
    ranks
        .iter()
        .map(|&rank| {
            let start = key_for(rank);
            let end = key_for(rank.saturating_add(SCAN_LENGTH as u64));
            (start, end)
        })
        .collect()
}

fn u64_key_bytes(value: u64) -> [u8; KEY_SIZE] {
    let mut buf = [0u8; KEY_SIZE];
    buf[..8].copy_from_slice(&value.to_le_bytes());
    buf
}

fn pregen_range_keys_u64_from_ranks(ranks: &[u64]) -> Vec<([u8; KEY_SIZE], [u8; KEY_SIZE])> {
    ranks
        .iter()
        .map(|&rank| {
            let start = rank;
            let end = rank.saturating_add(SCAN_LENGTH as u64);
            (u64_key_bytes(start), u64_key_bytes(end))
        })
        .collect()
}

fn pregen_u64_keys_bytes_from_ranks(ranks: &[u64]) -> Vec<[u8; KEY_SIZE]> {
    ranks.iter().map(|&rank| u64_key_bytes(rank)).collect()
}

fn pregen_all_keys_str() -> Vec<String> {
    (0..NUM_RECORDS).map(|i| key_for(i as u64)).collect()
}

fn pregen_all_keys_bytes() -> Vec<Vec<u8>> {
    pregen_all_keys_str()
        .into_iter()
        .map(|key| key.into_bytes())
        .collect()
}

fn concurrency_levels() -> Vec<usize> {
    let raw = env::var("SCALE_KV_CONCURRENCY").unwrap_or_default();
    if raw.trim().is_empty() {
        return Vec::new();
    }
    let mut levels = Vec::new();
    for part in raw.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Ok(value) = trimmed.parse::<usize>() {
            if value > 0 {
                levels.push(value);
            }
        }
    }
    levels
}

fn rpc_workers() -> usize {
    env::var("SCALE_KV_RPC_WORKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(1)
}



fn sled_enabled() -> bool {
    matches!(
        env::var("SCALE_KV_RUN_SLED").as_deref(),
        Ok("1") | Ok("true") | Ok("yes")
    )
}

fn batch_put_enabled() -> bool {
    !matches!(
        env::var("SCALE_KV_SKIP_BATCH_PUT").as_deref(),
        Ok("1") | Ok("true") | Ok("yes")
    )
}

fn in_memory_compute() -> bool {
    matches!(
        env::var("SCALE_KV_INMEMORY").as_deref(),
        Ok("1") | Ok("true") | Ok("yes")
    )
}

fn bench_set() -> Option<String> {
    env::var("SCALE_KV_BENCH_SET").ok().map(|v| v.to_lowercase())
}

fn bench_filter() -> Option<HashSet<String>> {
    let raw = env::var("SCALE_KV_BENCH_FILTER").ok()?;
    let mut set = HashSet::new();
    for part in raw.split(',') {
        let trimmed = part.trim();
        if !trimmed.is_empty() {
            set.insert(trimmed.to_string());
        }
    }
    if set.is_empty() {
        None
    } else {
        Some(set)
    }
}

fn bench_allowed(name: &str, is_sled: bool) -> bool {
    if let Some(set) = bench_set() {
        if is_sled && set != "sled" && set != "both" {
            return false;
        }
        if !is_sled && set != "scale_kv" && set != "both" {
            return false;
        }
    }
    if let Some(filter) = bench_filter() {
        return filter.contains(name);
    }
    true
}

fn criterion_config() -> Criterion {
    let mut criterion = Criterion::default();
    if let Ok(value) = env::var("SCALE_KV_SAMPLE_SIZE") {
        if let Ok(size) = value.parse::<usize>() {
            if size > 0 {
                criterion = criterion.sample_size(size);
            }
        }
    }
    if let Ok(value) = env::var("SCALE_KV_MEASUREMENT_SECS") {
        if let Ok(secs) = value.parse::<f64>() {
            if secs > 0.0 {
                criterion = criterion.measurement_time(Duration::from_secs_f64(secs));
            }
        }
    }
    if let Ok(value) = env::var("SCALE_KV_WARMUP_SECS") {
        if let Ok(secs) = value.parse::<f64>() {
            if secs > 0.0 {
                criterion = criterion.warm_up_time(Duration::from_secs_f64(secs));
            }
        }
    }
    criterion
}

fn bench_ycsb_a(c: &mut Criterion) {
    if !bench_allowed("workload_a", false) {
        return;
    }
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let local = LocalSet::new();
    let workers = rpc_workers();
    let compute = setup(&rt, &local, workers);
    let mut group = c.benchmark_group(format!("ycsb_network_workers{}", workers));
    group.bench_function("workload_a", |b| {
        let compute = compute.clone();
        b.iter(|| {
            let compute = compute.clone();
            local.block_on(&rt, async move {
                let mut rng = rand::thread_rng();
                let mut zipf = ZipfianGenerator::new(NUM_RECORDS as u64);
                for _ in 0..OPERATIONS {
                    let mut bool_buf = [0u8; 1];
                    rng.fill_bytes(&mut bool_buf);
                    let key_num = zipf.next(&mut rng);
                    let key = key_for(key_num);
                    if bool_buf[0] & 1 == 0 {
                        black_box(compute.get(&key).await.unwrap());
                    } else {
                        let mut new_value = vec![0u8; VALUE_SIZE];
                        rng.fill_bytes(&mut new_value);
                        compute.put(&key, &new_value).await.unwrap();
                    }
                }
            })
        })
    });
    group.finish();
}

fn bench_ycsb_b(c: &mut Criterion) {
    if !bench_allowed("workload_b", false) {
        return;
    }
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let local = LocalSet::new();
    let workers = rpc_workers();
    let compute = setup(&rt, &local, workers);
    let mut group = c.benchmark_group(format!("ycsb_network_workers{}", workers));
    group.bench_function("workload_b", |b| {
        let compute = compute.clone();
        b.iter(|| {
            let compute = compute.clone();
            local.block_on(&rt, async move {
                let mut rng = rand::thread_rng();
                let mut zipf = ZipfianGenerator::new(NUM_RECORDS as u64);
                for _ in 0..OPERATIONS {
                    let mut float_buf = [0u8; 8];
                    rng.fill_bytes(&mut float_buf);
                    let r: f64 = f64::from_le_bytes(float_buf).abs() % 1.0;
                    let key_num = zipf.next(&mut rng);
                    let key = key_for(key_num);
                    if r < 0.95 {
                        black_box(compute.get(&key).await.unwrap());
                    } else {
                        let mut new_value = vec![0u8; VALUE_SIZE];
                        rng.fill_bytes(&mut new_value);
                        compute.put(&key, &new_value).await.unwrap();
                    }
                }
            })
        })
    });
    group.finish();
}

fn bench_ycsb_c(c: &mut Criterion) {
    if !bench_allowed("workload_c", false) {
        return;
    }
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let local = LocalSet::new();
    let workers = rpc_workers();
    let compute = setup(&rt, &local, workers);
    let ranks = pregen_zipf_ranks();
    let keys = Arc::new(pregen_keys_str_from_ranks(&ranks));
    let mut group = c.benchmark_group(format!("ycsb_network_workers{}", workers));
    group.bench_function("workload_c", |b| {
        let compute = compute.clone();
        let keys = Arc::clone(&keys);
        b.iter(|| {
            let compute = compute.clone();
            let keys = Arc::clone(&keys);
            local.block_on(&rt, async move {
                for key in keys.iter() {
                    black_box(compute.get(key).await.unwrap());
                }
            })
        })
    });
    group.finish();
}

fn bench_ycsb_d(c: &mut Criterion) {
    if !bench_allowed("workload_d", false) {
        return;
    }
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let local = LocalSet::new();
    let workers = rpc_workers();
    let compute = setup(&rt, &local, workers);
    let ranks = pregen_zipf_ranks();
    let keys = Arc::new(pregen_latest_keys_str_from_ranks(&ranks));
    let mut group = c.benchmark_group(format!("ycsb_network_workers{}", workers));
    group.bench_function("workload_d", |b| {
        let compute = compute.clone();
        let keys = Arc::clone(&keys);
        b.iter(|| {
            let compute = compute.clone();
            let keys = Arc::clone(&keys);
            local.block_on(&rt, async move {
                for key in keys.iter() {
                    black_box(compute.get(key).await.unwrap());
                }
            })
        })
    });
    group.finish();
}

fn bench_ycsb_e(c: &mut Criterion) {
    if !bench_allowed("workload_e", false) {
        return;
    }
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let local = LocalSet::new();
    let workers = rpc_workers();
    let compute = setup(&rt, &local, workers);
    let ranks = pregen_zipf_ranks();
    let ranges = Arc::new(pregen_range_keys_str_from_ranks(&ranks));
    let mut group = c.benchmark_group(format!("ycsb_network_workers{}", workers));
    group.bench_function("workload_e", |b| {
        let compute = compute.clone();
        let ranges = Arc::clone(&ranges);
        b.iter(|| {
            let compute = compute.clone();
            let ranges = Arc::clone(&ranges);
            local.block_on(&rt, async move {
                for (start, end) in ranges.iter() {
                    let results = compute.range(start, end).await.unwrap();
                    black_box(results.len());
                }
            })
        })
    });
    group.finish();
}

fn bench_ycsb_f(c: &mut Criterion) {
    if !bench_allowed("workload_f", false) {
        return;
    }
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let local = LocalSet::new();
    let workers = rpc_workers();
    let compute = setup(&rt, &local, workers);
    let mut group = c.benchmark_group(format!("ycsb_network_workers{}", workers));
    group.bench_function("workload_f", |b| {
        let compute = compute.clone();
        b.iter(|| {
            let compute = compute.clone();
            local.block_on(&rt, async move {
                let mut rng = rand::thread_rng();
                let mut zipf = ZipfianGenerator::new(NUM_RECORDS as u64);
                for _ in 0..OPERATIONS {
                    let key_num = zipf.next(&mut rng);
                    let key = key_for(key_num);
                    if let Some(value) = compute.get(&key).await.unwrap() {
                        compute.put(&key, &value).await.unwrap();
                    }
                }
            })
        })
    });
    group.finish();
}

fn bench_throughput_put(c: &mut Criterion) {
    if !bench_allowed("throughput_put", false) {
        return;
    }
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let local = LocalSet::new();
    let workers = rpc_workers();
    let compute = setup(&rt, &local, workers);
    let mut group = c.benchmark_group(format!("ycsb_network_workers{}", workers));
    group.bench_function("throughput_put", |b| {
        let compute = compute.clone();
        b.iter(|| {
            let compute = compute.clone();
            local.block_on(&rt, async move {
                let value = vec![0u8; VALUE_SIZE];
                for i in 0..OPERATIONS {
                    let key = key_for_prefix("key", i as u64);
                    compute.put(&key, &value).await.unwrap();
                }
            })
        })
    });
    group.finish();
}

fn bench_throughput_get(c: &mut Criterion) {
    if !bench_allowed("throughput_get", false) {
        return;
    }
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let local = LocalSet::new();
    let workers = rpc_workers();
    let compute = setup(&rt, &local, workers);
    let next_key = Arc::new(AtomicUsize::new(0));
    let keys = pregen_all_keys_str();
    let mut group = c.benchmark_group(format!("ycsb_network_workers{}", workers));
    group.bench_function("throughput_get", |b| {
        let compute = compute.clone();
        let next_key = next_key.clone();
        let keys = keys.clone();
        b.iter(|| {
            let compute = compute.clone();
            let idx = next_key.fetch_add(1, Ordering::Relaxed) % keys.len();
            let key = &keys[idx];
            local.block_on(&rt, async move {
                black_box(compute.get(key).await.unwrap());
            })
        })
    });
    group.finish();
}


fn bench_batch_put(c: &mut Criterion) {
    if !bench_allowed("batch_put_16k", false) {
        return;
    }
    if !batch_put_enabled() {
        return;
    }
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let local = LocalSet::new();
    let workers = rpc_workers();
    let compute = setup(&rt, &local, workers);
    let mut group = c.benchmark_group(format!("ycsb_network_workers{}", workers));
    group.bench_function("batch_put_16k", |b| {
        let compute = compute.clone();
        b.iter(|| {
            let compute = compute.clone();
            local.block_on(&rt, async move {
                let mut items = Vec::with_capacity(BATCH_SIZE);
                for i in 0..BATCH_SIZE {
                    let key = key_for_prefix("batch", i as u64);
                    let value = vec![0u8; VALUE_SIZE];
                    items.push((key, value));
                }
                compute.batch_put(&items).await.unwrap();
            })
        })
    });
    group.finish();
}

fn bench_concurrency_workload_a(c: &mut Criterion) {
    let levels = concurrency_levels();
    if levels.is_empty() {
        return;
    }
    let workers = rpc_workers();
    for level in levels {
        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        let compute = setup(&rt, &local, workers);
        let mut group = c.benchmark_group("ycsb_network_concurrency");
        group.bench_function(format!("workload_a_conc{}", level), |b| {
            let compute = compute.clone();
            b.iter(|| {
                let compute = compute.clone();
                local.block_on(&rt, async move {
                    let per_worker = (OPERATIONS + level - 1) / level;
                    let mut tasks = Vec::with_capacity(level);
                    for _ in 0..level {
                        let compute = compute.clone();
                        tasks.push(tokio::task::spawn_local(async move {
                            let mut rng = rand::thread_rng();
                            let mut zipf = ZipfianGenerator::new(NUM_RECORDS as u64);
                            for _ in 0..per_worker {
                                let mut bool_buf = [0u8; 1];
                                rng.fill_bytes(&mut bool_buf);
                                let key_num = zipf.next(&mut rng);
                                let key = key_for(key_num);
                                if bool_buf[0] & 1 == 0 {
                                    let _ = compute.get(&key).await.unwrap();
                                } else {
                                    let mut new_value = vec![0u8; VALUE_SIZE];
                                    rng.fill_bytes(&mut new_value);
                                    compute.put(&key, &new_value).await.unwrap();
                                }
                            }
                        }));
                    }
                    let _ = futures::future::join_all(tasks).await;
                })
            })
        });
        group.finish();
    }
}

fn bench_bptree_get(c: &mut Criterion) {
    if !bench_allowed("bptree_get", false) {
        return;
    }
    let tree = setup_bptree();
    let ranks = pregen_zipf_ranks();
    let keys = pregen_keys_str_from_ranks(&ranks);
    let mut group = c.benchmark_group("bptree_inmemory");
    group.bench_function("bptree_get", |b| {
        let keys = keys.clone();
        b.iter(|| {
            for key in keys.iter() {
                black_box(tree.get(key.as_bytes()));
            }
        })
    });
    group.finish();
}

fn bench_bptree_range(c: &mut Criterion) {
    if !bench_allowed("bptree_range", false) {
        return;
    }
    let tree = setup_bptree();
    let ranks = pregen_zipf_ranks();
    let ranges = pregen_range_keys_str_from_ranks(&ranks);
    let mut group = c.benchmark_group("bptree_inmemory");
    group.bench_function("bptree_range", |b| {
        let ranges = ranges.clone();
        b.iter(|| {
            for (start, end) in ranges.iter() {
                let mut count = 0usize;
                tree.range_visit(start.as_bytes(), end.as_bytes(), |_k, _slot| {
                    count += 1;
                    true
                });
                black_box(count);
            }
        })
    });
    group.finish();
}

fn bench_bptree_get_u64(c: &mut Criterion) {
    if !bench_allowed("bptree_get_u64", false) {
        return;
    }
    let tree = setup_bptree_u64();
    let ranks = pregen_zipf_ranks();
    let keys = pregen_u64_keys_bytes_from_ranks(&ranks);
    let mut group = c.benchmark_group("bptree_inmemory_u64");
    group.bench_function("bptree_get_u64", |b| {
        let keys = keys.clone();
        b.iter(|| {
            for key in keys.iter() {
                black_box(tree.get(key));
            }
        })
    });
    group.finish();
}

fn bench_bptree_range_u64(c: &mut Criterion) {
    if !bench_allowed("bptree_range_u64", false) {
        return;
    }
    let tree = setup_bptree_u64();
    let ranks = pregen_zipf_ranks();
    let ranges = pregen_range_keys_u64_from_ranks(&ranks);
    let mut group = c.benchmark_group("bptree_inmemory_u64");
    group.bench_function("bptree_range_u64", |b| {
        let ranges = ranges.clone();
        b.iter(|| {
            for (start, end) in ranges.iter() {
                let mut count = 0usize;
                tree.range_visit(start, end, |_k, _slot| {
                    count += 1;
                    true
                });
                black_box(count);
            }
        })
    });
    group.finish();
}

fn bench_hashmap_get_u64(c: &mut Criterion) {
    if !bench_allowed("hashmap_get_u64", false) {
        return;
    }
    let map = setup_hashmap_u64();
    let ranks = pregen_zipf_ranks();
    let mut group = c.benchmark_group("hashmap_inmemory_u64");
    group.bench_function("hashmap_get_u64", |b| {
        let ranks = ranks.clone();
        b.iter(|| {
            for key_num in ranks.iter() {
                black_box(map.get(key_num));
            }
        })
    });
    group.finish();
}

fn bench_sled_workload_a(c: &mut Criterion) {
    if !sled_enabled() {
        return;
    }
    if !bench_allowed("workload_a", true) {
        return;
    }
    let db = setup_sled();
    let mut group = c.benchmark_group("sled_local");
    group.bench_function("workload_a", |b| {
        b.iter(|| {
            let mut rng = rand::thread_rng();
            let mut zipf = ZipfianGenerator::new(NUM_RECORDS as u64);
            for _ in 0..OPERATIONS {
                let mut bool_buf = [0u8; 1];
                rng.fill_bytes(&mut bool_buf);
                let key_num = zipf.next(&mut rng);
                let key = format!("user{:06}", key_num);
                if bool_buf[0] & 1 == 0 {
                    let _ = black_box(db.get(key.as_bytes()));
                } else {
                    let mut new_value = vec![0u8; VALUE_SIZE];
                    rng.fill_bytes(&mut new_value);
                    let _ = db.insert(key.as_bytes(), new_value.as_slice());
                }
            }
        })
    });
    group.finish();
}

fn bench_sled_workload_b(c: &mut Criterion) {
    if !sled_enabled() {
        return;
    }
    if !bench_allowed("workload_b", true) {
        return;
    }
    let db = setup_sled();
    let mut group = c.benchmark_group("sled_local");
    group.bench_function("workload_b", |b| {
        b.iter(|| {
            let mut rng = rand::thread_rng();
            let mut zipf = ZipfianGenerator::new(NUM_RECORDS as u64);
            for _ in 0..OPERATIONS {
                let mut float_buf = [0u8; 8];
                rng.fill_bytes(&mut float_buf);
                let r: f64 = f64::from_le_bytes(float_buf).abs() % 1.0;
                let key_num = zipf.next(&mut rng);
                let key = format!("user{:06}", key_num);
                if r < 0.95 {
                    let _ = black_box(db.get(key.as_bytes()));
                } else {
                    let mut new_value = vec![0u8; VALUE_SIZE];
                    rng.fill_bytes(&mut new_value);
                    let _ = db.insert(key.as_bytes(), new_value.as_slice());
                }
            }
        })
    });
    group.finish();
}

fn bench_sled_workload_c(c: &mut Criterion) {
    if !sled_enabled() {
        return;
    }
    if !bench_allowed("workload_c", true) {
        return;
    }
    let db = setup_sled();
    let ranks = pregen_zipf_ranks();
    let keys = pregen_keys_str_from_ranks(&ranks);
    let mut group = c.benchmark_group("sled_local");
    group.bench_function("workload_c", |b| {
        let keys = keys.clone();
        b.iter(|| {
            for key in keys.iter() {
                let _ = black_box(db.get(key.as_bytes()));
            }
        })
    });
    group.finish();
}

fn bench_sled_workload_d(c: &mut Criterion) {
    if !sled_enabled() {
        return;
    }
    if !bench_allowed("workload_d", true) {
        return;
    }
    let db = setup_sled();
    let ranks = pregen_zipf_ranks();
    let keys = pregen_latest_keys_str_from_ranks(&ranks);
    let mut group = c.benchmark_group("sled_local");
    group.bench_function("workload_d", |b| {
        let keys = keys.clone();
        b.iter(|| {
            for key in keys.iter() {
                let _ = black_box(db.get(key.as_bytes()));
            }
        })
    });
    group.finish();
}

fn bench_sled_workload_e(c: &mut Criterion) {
    if !sled_enabled() {
        return;
    }
    if !bench_allowed("workload_e", true) {
        return;
    }
    let db = setup_sled();
    let ranks = pregen_zipf_ranks();
    let ranges = pregen_range_keys_str_from_ranks(&ranks);
    let mut group = c.benchmark_group("sled_local");
    group.bench_function("workload_e", |b| {
        let ranges = ranges.clone();
        b.iter(|| {
            for (start, end) in ranges.iter() {
                let mut count = 0usize;
                for _ in db
                    .range(start.as_bytes()..=end.as_bytes())
                    .take(SCAN_LENGTH)
                {
                    count += 1;
                }
                black_box(count);
            }
        })
    });
    group.finish();
}

fn bench_sled_workload_f(c: &mut Criterion) {
    if !sled_enabled() {
        return;
    }
    if !bench_allowed("workload_f", true) {
        return;
    }
    let db = setup_sled();
    let mut group = c.benchmark_group("sled_local");
    group.bench_function("workload_f", |b| {
        b.iter(|| {
            let mut rng = rand::thread_rng();
            let mut zipf = ZipfianGenerator::new(NUM_RECORDS as u64);
            for _ in 0..OPERATIONS {
                let key_num = zipf.next(&mut rng);
                let key = key_for(key_num);
                if let Ok(Some(value)) = db.get(key.as_bytes()) {
                    let _ = db.insert(key.as_bytes(), value.as_ref());
                }
            }
        })
    });
    group.finish();
}

fn bench_sled_throughput_put(c: &mut Criterion) {
    if !sled_enabled() {
        return;
    }
    if !bench_allowed("throughput_put", true) {
        return;
    }
    let dir = tempfile::TempDir::new().unwrap();
    let db = Config::new().path(dir.path()).open().unwrap();
    let mut group = c.benchmark_group("sled_local");
    group.bench_function("throughput_put", |b| {
        b.iter(|| {
            let value = vec![0u8; VALUE_SIZE];
            for i in 0..OPERATIONS {
                let key = key_for_prefix("key", i as u64);
                let _ = db.insert(key.as_bytes(), value.as_slice());
            }
        })
    });
    group.finish();
}

fn bench_sled_throughput_get(c: &mut Criterion) {
    if !sled_enabled() {
        return;
    }
    if !bench_allowed("throughput_get", true) {
        return;
    }
    let db = setup_sled();
    let next_key = Arc::new(AtomicUsize::new(0));
    let keys = pregen_all_keys_bytes();
    let mut group = c.benchmark_group("sled_local");
    group.bench_function("throughput_get", |b| {
        let next_key = next_key.clone();
        let keys = keys.clone();
        b.iter(|| {
            let idx = next_key.fetch_add(1, Ordering::Relaxed) % keys.len();
            let _ = black_box(db.get(&keys[idx]));
        })
    });
    group.finish();
}

fn bench_sled_batch_put(c: &mut Criterion) {
    if !sled_enabled() {
        return;
    }
    if !bench_allowed("batch_put_16k", true) {
        return;
    }
    if !batch_put_enabled() {
        return;
    }
    let dir = tempfile::TempDir::new().unwrap();
    let db = Config::new().path(dir.path()).open().unwrap();
    let mut group = c.benchmark_group("sled_local");
    group.bench_function("batch_put_16k", |b| {
        b.iter(|| {
            let mut batch = sled::Batch::default();
            for i in 0..BATCH_SIZE {
                let key = key_for_prefix("batch", i as u64);
                let value = vec![0u8; VALUE_SIZE];
                batch.insert(key.as_bytes(), value);
            }
            let _ = db.apply_batch(batch);
        })
    });
    group.finish();
}

criterion_group!(
    name = benches;
    config = criterion_config();
    targets =
    bench_ycsb_a,
    bench_ycsb_b,
    bench_ycsb_c,
    bench_ycsb_d,
    bench_ycsb_e,
    bench_ycsb_f,
    bench_throughput_put,
    bench_throughput_get,
    bench_batch_put,
    bench_sled_workload_a,
    bench_sled_workload_b,
    bench_sled_workload_c,
    bench_sled_workload_d,
    bench_sled_workload_e,
    bench_sled_workload_f,
    bench_sled_throughput_put,
    bench_sled_throughput_get,
    bench_sled_batch_put,
    bench_concurrency_workload_a,
    bench_bptree_get,
    bench_bptree_range,
    bench_bptree_get_u64,
    bench_bptree_range_u64,
    bench_hashmap_get_u64
);
criterion_main!(benches);
