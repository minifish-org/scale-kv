// YCSB-style end-to-end benchmark for scale-kv (network path)
// Tests: ComputeNode → TCP → StorageServer → StorageNode
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::RngCore;
use scale_kv::ComputeNode;
use std::net::SocketAddr;
use std::thread;
use std::time::Duration;

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

/// Start a storage server and return its address
fn start_storage_server() -> SocketAddr {
    use std::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let data = std::sync::Arc::new(scale_kv::StorageNode::new());

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            if let Ok(stream) = stream {
                let _ = scale_kv::server::handle_connection(stream, data.clone());
            }
        }
    });

    thread::sleep(Duration::from_millis(10));
    addr
}

/// YCSB Workload A: 50% read, 50% update (full network path)
fn bench_ycsb_a(c: &mut Criterion) {
    const NUM_RECORDS: usize = 10_000;
    const OPERATIONS: usize = 1_000;
    const VALUE_SIZE: usize = 1024;

    let addr = start_storage_server();
    let value = vec![0u8; VALUE_SIZE];

    let mut compute = ComputeNode::with_storage(&addr.to_string());
    for i in 0..NUM_RECORDS {
        let key = format!("user{:06}", i);
        compute.put(&key, &value);
    }

    let mut group = c.benchmark_group("ycsb_network");
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
                    black_box(compute.get(&key));
                } else {
                    let mut new_value = vec![0u8; VALUE_SIZE];
                    rng.fill_bytes(&mut new_value);
                    compute.put(&key, &new_value);
                }
            }
        })
    });
    group.finish();
}

/// YCSB Workload B: 95% read, 5% update
fn bench_ycsb_b(c: &mut Criterion) {
    const NUM_RECORDS: usize = 10_000;
    const OPERATIONS: usize = 1_000;
    const VALUE_SIZE: usize = 1024;

    let addr = start_storage_server();
    let value = vec![0u8; VALUE_SIZE];

    let mut compute = ComputeNode::with_storage(&addr.to_string());
    for i in 0..NUM_RECORDS {
        let key = format!("user{:06}", i);
        compute.put(&key, &value);
    }

    let mut group = c.benchmark_group("ycsb_network");
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
                    black_box(compute.get(&key));
                } else {
                    let mut new_value = vec![0u8; VALUE_SIZE];
                    rng.fill_bytes(&mut new_value);
                    compute.put(&key, &new_value);
                }
            }
        })
    });
    group.finish();
}

/// YCSB Workload C: 100% read
fn bench_ycsb_c(c: &mut Criterion) {
    const NUM_RECORDS: usize = 10_000;
    const OPERATIONS: usize = 1_000;

    let addr = start_storage_server();
    let value = vec![0u8; 1024];

    let mut compute = ComputeNode::with_storage(&addr.to_string());
    for i in 0..NUM_RECORDS {
        let key = format!("user{:06}", i);
        compute.put(&key, &value);
    }

    let mut group = c.benchmark_group("ycsb_network");
    group.bench_function("workload_c", |b| {
        b.iter(|| {
            let mut rng = rand::thread_rng();
            let mut zipf = ZipfianGenerator::new(NUM_RECORDS as u64);

            for _ in 0..OPERATIONS {
                let key_num = zipf.next(&mut rng);
                let key = format!("user{:06}", key_num);
                black_box(compute.get(&key));
            }
        })
    });
    group.finish();
}

/// Throughput test: sequential put
fn bench_throughput_put(c: &mut Criterion) {
    const OPERATIONS: usize = 1_000;

    let addr = start_storage_server();
    let value = vec![0u8; 1024];
    let mut compute = ComputeNode::with_storage(&addr.to_string());

    let mut group = c.benchmark_group("ycsb_network");
    group.bench_function("throughput_put", |b| {
        b.iter(|| {
            for i in 0..OPERATIONS {
                let key = format!("key{:08}", i);
                compute.put(&key, &value);
            }
        })
    });
    group.finish();
}

/// Throughput test: sequential get
fn bench_throughput_get(c: &mut Criterion) {
    const OPERATIONS: usize = 1_000;

    let addr = start_storage_server();
    let value = vec![0u8; 1024];
    let mut compute = ComputeNode::with_storage(&addr.to_string());

    for i in 0..OPERATIONS {
        let key = format!("key{:08}", i);
        compute.put(&key, &value);
    }

    let mut group = c.benchmark_group("ycsb_network");
    group.bench_function("throughput_get", |b| {
        b.iter(|| {
            for i in 0..OPERATIONS {
                let key = format!("key{:08}", i);
                black_box(compute.get(&key));
            }
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
