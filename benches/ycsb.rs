use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::RngCore;
use scale_kv::{ComputeNode, StorageServer};
use sled::Config;
use std::collections::HashSet;
use std::time::Duration;
use std::net::SocketAddr;
use std::sync::Arc;
use std::env;
use tokio::runtime::Builder;
use tokio::task::LocalSet;

const NUM_RECORDS: usize = 10_000;
const OPERATIONS: usize = 1_000;
const VALUE_SIZE: usize = 1024;
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
    format!("user{:06}", id)
}

fn latest_key(zipf: &mut ZipfianGenerator, rng: &mut impl RngCore) -> String {
    let rank = zipf.next(rng) as usize;
    let max = NUM_RECORDS.saturating_sub(1);
    let key_num = max.saturating_sub(rank.min(max));
    key_for(key_num as u64)
}

fn setup(rt: &tokio::runtime::Runtime, local: &LocalSet, workers: usize) -> Arc<ComputeNode> {
    local.block_on(rt, async {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = StorageServer::start(addr).await.unwrap();
        let compute = ComputeNode::with_storage_workers(&server.addr().to_string(), workers, local)
            .await
            .unwrap();
        let value = vec![0u8; VALUE_SIZE];
        for i in 0..NUM_RECORDS {
            let key = key_for(i as u64);
            compute.put(&key, &value).await.unwrap();
        }
        Arc::new(compute)
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
    let mut group = c.benchmark_group(format!("ycsb_network_workers{}", workers));
    group.bench_function("workload_c", |b| {
        let compute = compute.clone();
        b.iter(|| {
            let compute = compute.clone();
            local.block_on(&rt, async move {
                let mut rng = rand::thread_rng();
                let mut zipf = ZipfianGenerator::new(NUM_RECORDS as u64);
                for _ in 0..OPERATIONS {
                    let key_num = zipf.next(&mut rng);
                    let key = key_for(key_num);
                    black_box(compute.get(&key).await.unwrap());
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
    let mut group = c.benchmark_group(format!("ycsb_network_workers{}", workers));
    group.bench_function("workload_d", |b| {
        let compute = compute.clone();
        b.iter(|| {
            let compute = compute.clone();
            local.block_on(&rt, async move {
                let mut rng = rand::thread_rng();
                let mut zipf = ZipfianGenerator::new(NUM_RECORDS as u64);
                for _ in 0..OPERATIONS {
                    let key = latest_key(&mut zipf, &mut rng);
                    black_box(compute.get(&key).await.unwrap());
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
    let mut group = c.benchmark_group(format!("ycsb_network_workers{}", workers));
    group.bench_function("workload_e", |b| {
        let compute = compute.clone();
        b.iter(|| {
            let compute = compute.clone();
            local.block_on(&rt, async move {
                let mut rng = rand::thread_rng();
                let mut zipf = ZipfianGenerator::new(NUM_RECORDS as u64);
                for _ in 0..OPERATIONS {
                    let key_num = zipf.next(&mut rng);
                    let start = key_for(key_num);
                    let end = key_for(key_num.saturating_add(SCAN_LENGTH as u64));
                    let _ = compute.range(&start, &end).await.unwrap();
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
                    let key = format!("key{:08}", i);
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
    let mut group = c.benchmark_group(format!("ycsb_network_workers{}", workers));
    group.bench_function("throughput_get", |b| {
        let compute = compute.clone();
        b.iter(|| {
            let compute = compute.clone();
            local.block_on(&rt, async move {
                for i in 0..OPERATIONS {
                    let key = key_for(i as u64);
                    black_box(compute.get(&key).await.unwrap());
                }
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
                    let key = format!("batch{:08}", i);
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
    let mut group = c.benchmark_group("sled_local");
    group.bench_function("workload_c", |b| {
        b.iter(|| {
            let mut rng = rand::thread_rng();
            let mut zipf = ZipfianGenerator::new(NUM_RECORDS as u64);
            for _ in 0..OPERATIONS {
                let key_num = zipf.next(&mut rng);
                let key = format!("user{:06}", key_num);
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
    let mut group = c.benchmark_group("sled_local");
    group.bench_function("workload_d", |b| {
        b.iter(|| {
            let mut rng = rand::thread_rng();
            let mut zipf = ZipfianGenerator::new(NUM_RECORDS as u64);
            for _ in 0..OPERATIONS {
                let key = latest_key(&mut zipf, &mut rng);
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
    let mut group = c.benchmark_group("sled_local");
    group.bench_function("workload_e", |b| {
        b.iter(|| {
            let mut rng = rand::thread_rng();
            let mut zipf = ZipfianGenerator::new(NUM_RECORDS as u64);
            for _ in 0..OPERATIONS {
                let key_num = zipf.next(&mut rng);
                let start = key_for(key_num);
                let end = key_for(key_num.saturating_add(SCAN_LENGTH as u64));
                let _ = black_box(db.range(start.as_bytes()..=end.as_bytes()).take(SCAN_LENGTH));
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
                let key = format!("key{:08}", i);
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
    let mut group = c.benchmark_group("sled_local");
    group.bench_function("throughput_get", |b| {
        b.iter(|| {
            for i in 0..OPERATIONS {
                let key = format!("user{:06}", i);
                let _ = black_box(db.get(key.as_bytes()));
            }
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
                let key = format!("batch{:08}", i);
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
    bench_concurrency_workload_a
);
criterion_main!(benches);
