use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::RngCore;
use scale_kv::{ComputeNode, StorageServer};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::runtime::Builder;
use tokio::task::LocalSet;

const NUM_RECORDS: usize = 10_000;
const OPERATIONS: usize = 1_000;
const VALUE_SIZE: usize = 1024;

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

fn setup(rt: &tokio::runtime::Runtime, local: &LocalSet) -> Arc<ComputeNode> {
    local.block_on(rt, async {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = StorageServer::start(addr).await.unwrap();
        let compute = ComputeNode::with_storage(&server.addr().to_string())
            .await
            .unwrap();
        let value = vec![0u8; VALUE_SIZE];
        for i in 0..NUM_RECORDS {
            let key = format!("user{:06}", i);
            compute.put(&key, &value).await.unwrap();
        }
        Arc::new(compute)
    })
}

fn bench_ycsb_a(c: &mut Criterion) {
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let local = LocalSet::new();
    let compute = setup(&rt, &local);
    let mut group = c.benchmark_group("ycsb_network");
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
                    let key = format!("user{:06}", key_num);
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
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let local = LocalSet::new();
    let compute = setup(&rt, &local);
    let mut group = c.benchmark_group("ycsb_network");
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
                    let key = format!("user{:06}", key_num);
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
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let local = LocalSet::new();
    let compute = setup(&rt, &local);
    let mut group = c.benchmark_group("ycsb_network");
    group.bench_function("workload_c", |b| {
        let compute = compute.clone();
        b.iter(|| {
            let compute = compute.clone();
            local.block_on(&rt, async move {
                let mut rng = rand::thread_rng();
                let mut zipf = ZipfianGenerator::new(NUM_RECORDS as u64);
                for _ in 0..OPERATIONS {
                    let key_num = zipf.next(&mut rng);
                    let key = format!("user{:06}", key_num);
                    black_box(compute.get(&key).await.unwrap());
                }
            })
        })
    });
    group.finish();
}

fn bench_throughput_put(c: &mut Criterion) {
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let local = LocalSet::new();
    let compute = setup(&rt, &local);
    let mut group = c.benchmark_group("ycsb_network");
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
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let local = LocalSet::new();
    let compute = setup(&rt, &local);
    let mut group = c.benchmark_group("ycsb_network");
    group.bench_function("throughput_get", |b| {
        let compute = compute.clone();
        b.iter(|| {
            let compute = compute.clone();
            local.block_on(&rt, async move {
                for i in 0..OPERATIONS {
                    let key = format!("user{:06}", i);
                    black_box(compute.get(&key).await.unwrap());
                }
            })
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_ycsb_a,
    bench_ycsb_b,
    bench_ycsb_c,
    bench_throughput_put,
    bench_throughput_get
);
criterion_main!(benches);
