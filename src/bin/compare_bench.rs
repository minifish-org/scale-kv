use anyhow::{Context, bail};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use redb::{Database, TableDefinition};
use scale_kv::{EmbeddedCompute, Error, ErrorCategory, KEY_SIZE, StorageServer, VALUE_SIZE};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::task::LocalSet;

const REDB_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");

#[derive(Clone, Copy, Debug)]
enum BackendKind {
    ScaleKv,
    Redb,
}

#[derive(Clone, Copy, Debug)]
enum BenchMode {
    Put,
    Get,
    Scan,
}

#[derive(Clone, Debug)]
struct Config {
    backend: BackendKind,
    mode: BenchMode,
    clients: usize,
    duration_secs: u64,
    keyspace: u64,
    preload_keys: u64,
    preload: bool,
    preload_only: bool,
    allow_misses: bool,
    value_size: usize,
    txn_ops: usize,
    scan_len: usize,
    seed: u64,
}

#[derive(Clone)]
enum SharedBackend {
    ScaleKv(EmbeddedCompute),
    Redb(Arc<Database>),
}

enum BackendState {
    ScaleKv {
        _server: StorageServer,
        _dir: TempDir,
    },
    Redb {
        _dir: TempDir,
    },
}

#[derive(Default)]
struct WorkerStats {
    reads: u64,
    writes: u64,
    retryable_read_errors: u64,
    retryable_write_errors: u64,
    samples: Vec<u64>,
}

struct Reservoir {
    cap: usize,
    seen: u64,
    samples: Vec<u64>,
}

impl Reservoir {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            seen: 0,
            samples: Vec::with_capacity(cap),
        }
    }

    fn record(&mut self, v: u64, rng: &mut StdRng) {
        self.seen += 1;
        if self.samples.len() < self.cap {
            self.samples.push(v);
            return;
        }

        let idx = rng.gen_range(0..self.seen);
        if (idx as usize) < self.cap {
            self.samples[idx as usize] = v;
        }
    }
}

fn key_for(id: u64) -> [u8; KEY_SIZE] {
    let mut key = [0u8; KEY_SIZE];
    key[..8].copy_from_slice(&id.to_be_bytes());
    key
}

fn value_for(seed: u64, value_size: usize) -> Vec<u8> {
    let mut value = vec![0u8; value_size];
    if value_size >= 8 {
        value[..8].copy_from_slice(&seed.to_le_bytes());
    }
    value
}

fn scan_bounds(start: u64, keyspace: u64, scan_len: usize) -> ([u8; KEY_SIZE], [u8; KEY_SIZE]) {
    let max_idx = keyspace.saturating_sub(1);
    let len = scan_len.max(1) as u64;
    let end = start.saturating_add(len.saturating_sub(1)).min(max_idx);
    (key_for(start), key_for(end))
}

fn parse_mode(s: &str) -> anyhow::Result<BenchMode> {
    match s {
        "put" => Ok(BenchMode::Put),
        "get" => Ok(BenchMode::Get),
        "scan" => Ok(BenchMode::Scan),
        other => bail!("invalid --mode: {other} (expected put|get|scan)"),
    }
}

fn parse_config() -> anyhow::Result<Config> {
    let args: Vec<String> = std::env::args().collect();

    let mut cfg = Config {
        backend: BackendKind::ScaleKv,
        mode: BenchMode::Get,
        clients: 8,
        duration_secs: 10,
        keyspace: 100_000,
        preload_keys: 100_000,
        preload: true,
        preload_only: false,
        allow_misses: false,
        value_size: VALUE_SIZE,
        txn_ops: 16,
        scan_len: 64,
        seed: rand::random::<u64>(),
    };

    let mut preload_keys_set = false;
    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--backend" => {
                i += 1;
                let val = args.get(i).context("missing value for --backend")?;
                cfg.backend = match val.as_str() {
                    "scale-kv" => BackendKind::ScaleKv,
                    "redb" => BackendKind::Redb,
                    other => bail!("invalid --backend: {other} (expected scale-kv|redb)"),
                };
            }
            "--mode" => {
                i += 1;
                let val = args.get(i).context("missing value for --mode")?;
                cfg.mode = parse_mode(val)?;
            }
            "--clients" => {
                i += 1;
                cfg.clients = args
                    .get(i)
                    .context("missing value for --clients")?
                    .parse::<usize>()
                    .context("invalid --clients")?
                    .max(1);
            }
            "--duration-secs" => {
                i += 1;
                cfg.duration_secs = args
                    .get(i)
                    .context("missing value for --duration-secs")?
                    .parse::<u64>()
                    .context("invalid --duration-secs")?
                    .max(1);
            }
            "--keyspace" => {
                i += 1;
                cfg.keyspace = args
                    .get(i)
                    .context("missing value for --keyspace")?
                    .parse::<u64>()
                    .context("invalid --keyspace")?;
            }
            "--preload-keys" => {
                i += 1;
                cfg.preload_keys = args
                    .get(i)
                    .context("missing value for --preload-keys")?
                    .parse::<u64>()
                    .context("invalid --preload-keys")?;
                preload_keys_set = true;
            }
            "--preload" => {
                cfg.preload = true;
            }
            "--skip-preload" => {
                cfg.preload = false;
            }
            "--preload-only" => {
                cfg.preload_only = true;
            }
            "--allow-misses" => {
                cfg.allow_misses = true;
            }
            "--value-size" => {
                i += 1;
                cfg.value_size = args
                    .get(i)
                    .context("missing value for --value-size")?
                    .parse::<usize>()
                    .context("invalid --value-size")?;
            }
            "--txn-ops" => {
                i += 1;
                cfg.txn_ops = args
                    .get(i)
                    .context("missing value for --txn-ops")?
                    .parse::<usize>()
                    .context("invalid --txn-ops")?;
            }
            "--scan-len" => {
                i += 1;
                cfg.scan_len = args
                    .get(i)
                    .context("missing value for --scan-len")?
                    .parse::<usize>()
                    .context("invalid --scan-len")?;
            }
            "--seed" => {
                if i + 1 < args.len() && !args[i + 1].starts_with("--") {
                    i += 1;
                    cfg.seed = args
                        .get(i)
                        .context("missing value for --seed")?
                        .parse::<u64>()
                        .context("invalid --seed")?;
                } else {
                    cfg.seed = 0;
                }
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
        i += 1;
    }

    if cfg.keyspace == 0 {
        bail!("--keyspace must be > 0");
    }
    if !preload_keys_set {
        cfg.preload_keys = cfg.keyspace;
    }
    if cfg.preload_keys > cfg.keyspace {
        cfg.preload_keys = cfg.keyspace;
    }
    if !cfg.preload_only && !cfg.allow_misses && cfg.preload_keys == 0 {
        bail!("--preload-keys must be > 0 unless --allow-misses or --preload-only is set");
    }
    if cfg.txn_ops == 0 {
        bail!("--txn-ops must be > 0");
    }
    if cfg.scan_len == 0 {
        bail!("--scan-len must be > 0");
    }
    if matches!(cfg.backend, BackendKind::ScaleKv) && cfg.value_size != VALUE_SIZE {
        bail!(
            "scale-kv requires fixed --value-size {} (got {})",
            VALUE_SIZE,
            cfg.value_size
        );
    }

    Ok(cfg)
}

fn print_help() {
    println!(
        "Usage: compare_bench [--backend scale-kv|redb] [--mode put|get|scan] [--clients N] [--duration-secs S] [--keyspace K] \\\n[--preload-keys N] [--preload] [--skip-preload] [--preload-only] [--allow-misses] [--value-size BYTES] [--txn-ops N] [--scan-len N] [--seed [SEED]]"
    );
}

fn percentile_us(values: &mut [u64], percentile: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    let rank = ((values.len() - 1) as f64 * percentile).round() as usize;
    values[rank.min(values.len() - 1)]
}

fn is_retryable_backpressure(err: &Error) -> bool {
    if err.category() == ErrorCategory::Backpressure {
        return true;
    }
    let msg = err.to_string();
    msg.contains("WouldBlock") || msg.contains("backpressure")
}

async fn init_backend(
    cfg: &Config,
    local: &LocalSet,
) -> anyhow::Result<(BackendState, SharedBackend)> {
    match cfg.backend {
        BackendKind::ScaleKv => {
            let dir = tempfile::tempdir().context("create temp dir for scale-kv")?;
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let server = StorageServer::start_with_dir(addr, dir.path().to_path_buf()).await?;
            let addrs = vec![server.addr().to_string()];
            let compute = EmbeddedCompute::connect(&addrs, 1, local).await?;
            Ok((
                BackendState::ScaleKv {
                    _server: server,
                    _dir: dir,
                },
                SharedBackend::ScaleKv(compute),
            ))
        }
        BackendKind::Redb => {
            let dir = tempfile::tempdir().context("create temp dir for redb")?;
            let path = dir.path().join("compare.redb");
            let db = Arc::new(Database::create(path).context("open redb database")?);

            {
                let write_txn = db.begin_write().context("redb begin_write")?;
                {
                    let _ = write_txn
                        .open_table(REDB_TABLE)
                        .context("redb open table")?;
                }
                write_txn.commit().context("redb commit table create")?;
            }

            Ok((BackendState::Redb { _dir: dir }, SharedBackend::Redb(db)))
        }
    }
}

async fn preload(backend: &SharedBackend, cfg: &Config) -> anyhow::Result<()> {
    const PRELOAD_LOG_INTERVAL: u64 = 10_000;
    let preload_keys = cfg.preload_keys.min(cfg.keyspace);
    if preload_keys == 0 {
        return Ok(());
    }

    match backend {
        SharedBackend::ScaleKv(compute) => {
            let preload_batch = cfg.txn_ops.max(1) as u64;
            let mut next = 0u64;
            while next < preload_keys {
                let batch_end = (next + preload_batch).min(preload_keys);
                loop {
                    let mut tx = compute.begin_rw().await;
                    let mut retry_batch = false;
                    for i in next..batch_end {
                        let key = key_for(i);
                        let value = value_for(i, cfg.value_size);
                        match tx.put(&key, &value).await {
                            Ok(()) => {}
                            Err(err) if is_retryable_backpressure(&err) => {
                                retry_batch = true;
                                break;
                            }
                            Err(err) => return Err(err.into()),
                        }
                    }

                    if retry_batch {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        continue;
                    }

                    match tx.commit().await {
                        Ok(_) => break,
                        Err(err) if is_retryable_backpressure(&err) => {
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                        Err(err) => return Err(err.into()),
                    }
                }
                next = batch_end;
                if next % PRELOAD_LOG_INTERVAL == 0 || next == preload_keys {
                    println!("preload_progress loaded={} total={}", next, preload_keys);
                }
            }
        }
        SharedBackend::Redb(db) => {
            let db = Arc::clone(db);
            let keyspace = preload_keys;
            let value_size = cfg.value_size;
            tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                const PRELOAD_TXN_BATCH: u64 = 1024;
                let mut next = 0u64;
                while next < keyspace {
                    let write_txn = db.begin_write().context("redb begin_write preload")?;
                    {
                        let mut table = write_txn
                            .open_table(REDB_TABLE)
                            .context("redb open table preload")?;
                        for _ in 0..PRELOAD_TXN_BATCH {
                            if next >= keyspace {
                                break;
                            }
                            let key = key_for(next);
                            let value = value_for(next, value_size);
                            table
                                .insert(key.as_slice(), value.as_slice())
                                .context("redb preload insert")?;
                            next += 1;
                        }
                    }
                    write_txn.commit().context("redb preload commit")?;
                    if next % PRELOAD_LOG_INTERVAL == 0 || next == keyspace {
                        println!("preload_progress loaded={} total={}", next, keyspace);
                    }
                }
                Ok(())
            })
            .await
            .context("join redb preload task")??;
        }
    }

    Ok(())
}

#[derive(Clone)]
enum TxnPlan {
    Put(Vec<([u8; KEY_SIZE], Vec<u8>)>),
    Get(Vec<[u8; KEY_SIZE]>),
    Scan(Vec<([u8; KEY_SIZE], [u8; KEY_SIZE], usize)>),
}

async fn do_scale_get_txn(
    compute: &EmbeddedCompute,
    keys: &[[u8; KEY_SIZE]],
    deadline: Instant,
    retryable_read_errors: &mut u64,
) -> anyhow::Result<Option<Duration>> {
    let start = Instant::now();
    'retry: loop {
        if Instant::now() >= deadline {
            return Ok(None);
        }

        let mut tx = compute.begin_ro_timeout(Duration::from_secs(5));
        for key in keys {
            match tx.get(key).await {
                Ok(_) => {}
                Err(err) if is_retryable_backpressure(&err) => {
                    *retryable_read_errors += 1;
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue 'retry;
                }
                Err(err) => return Err(err.into()),
            }
        }

        return Ok(Some(start.elapsed()));
    }
}

async fn do_scale_scan_txn(
    compute: &EmbeddedCompute,
    ranges: &[([u8; KEY_SIZE], [u8; KEY_SIZE], usize)],
    deadline: Instant,
    retryable_read_errors: &mut u64,
) -> anyhow::Result<Option<Duration>> {
    let start = Instant::now();
    'retry: loop {
        if Instant::now() >= deadline {
            return Ok(None);
        }

        let mut tx = compute.begin_ro_timeout(Duration::from_secs(5));
        for (range_start, range_end, limit) in ranges {
            match tx.scan_range(range_start, range_end, *limit).await {
                Ok(_) => {}
                Err(err) if is_retryable_backpressure(&err) => {
                    *retryable_read_errors += 1;
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue 'retry;
                }
                Err(err) => return Err(err.into()),
            }
        }

        return Ok(Some(start.elapsed()));
    }
}

async fn do_scale_put_txn(
    compute: &EmbeddedCompute,
    writes: &[([u8; KEY_SIZE], Vec<u8>)],
    deadline: Instant,
    retryable_write_errors: &mut u64,
) -> anyhow::Result<Option<Duration>> {
    let start = Instant::now();
    'retry: loop {
        if Instant::now() >= deadline {
            return Ok(None);
        }

        let mut tx = compute.begin_rw().await;
        for (key, value) in writes {
            match tx.put(key, value).await {
                Ok(()) => {}
                Err(err) if is_retryable_backpressure(&err) => {
                    *retryable_write_errors += 1;
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue 'retry;
                }
                Err(err) => return Err(err.into()),
            }
        }

        match tx.commit().await {
            Ok(_) => return Ok(Some(start.elapsed())),
            Err(err) if is_retryable_backpressure(&err) => {
                *retryable_write_errors += 1;
                tokio::time::sleep(Duration::from_millis(1)).await;
                continue;
            }
            Err(err) => return Err(err.into()),
        }
    }
}

async fn do_redb_txn(db: Arc<Database>, plan: TxnPlan) -> anyhow::Result<Duration> {
    let start = Instant::now();
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        match plan {
            TxnPlan::Put(writes) => {
                let write_txn = db.begin_write().context("redb begin_write put")?;
                {
                    let mut table = write_txn
                        .open_table(REDB_TABLE)
                        .context("redb open table put")?;
                    for (key, value) in writes {
                        table
                            .insert(key.as_slice(), value.as_slice())
                            .context("redb put insert")?;
                    }
                }
                write_txn.commit().context("redb put commit")?;
            }
            TxnPlan::Get(keys) => {
                let read_txn = db.begin_read().context("redb begin_read get")?;
                let table = read_txn
                    .open_table(REDB_TABLE)
                    .context("redb open table get")?;
                for key in keys {
                    let _ = table.get(key.as_slice()).context("redb get")?;
                }
            }
            TxnPlan::Scan(ranges) => {
                let read_txn = db.begin_read().context("redb begin_read scan")?;
                let table = read_txn
                    .open_table(REDB_TABLE)
                    .context("redb open table scan")?;
                for (range_start, range_end, limit) in ranges {
                    let mut iter = table
                        .range(range_start.as_slice()..=range_end.as_slice())
                        .context("redb range")?;
                    for _ in 0..limit {
                        if iter.next().transpose().context("redb scan next")?.is_none() {
                            break;
                        }
                    }
                }
            }
        }
        Ok(())
    })
    .await
    .context("join redb txn task")??;

    Ok(start.elapsed())
}

fn mode_name(mode: BenchMode) -> &'static str {
    match mode {
        BenchMode::Put => "put",
        BenchMode::Get => "get",
        BenchMode::Scan => "scan",
    }
}

fn effective_keyspace(cfg: &Config) -> u64 {
    if cfg.allow_misses {
        cfg.keyspace
    } else {
        cfg.preload_keys.min(cfg.keyspace)
    }
}

async fn run_workers(backend: SharedBackend, cfg: &Config) -> anyhow::Result<WorkerStats> {
    let mut join_handles = Vec::with_capacity(cfg.clients);
    let duration = Duration::from_secs(cfg.duration_secs);
    let deadline = Instant::now() + duration;

    for worker in 0..cfg.clients {
        let backend = backend.clone();
        let mode = cfg.mode;
        let keyspace = effective_keyspace(cfg);
        let txn_ops = cfg.txn_ops;
        let scan_len = cfg.scan_len;
        let value_size = cfg.value_size;
        let worker_seed = cfg.seed ^ ((worker as u64 + 1).wrapping_mul(0x9e3779b97f4a7c15));
        let worker_deadline = deadline;

        join_handles.push(tokio::task::spawn_local(async move {
            let mut rng = StdRng::seed_from_u64(worker_seed);
            let mut stats = WorkerStats::default();
            let mut reservoir = Reservoir::new(20_000);
            let mut write_seq = 0u64;
            let mut txn_count = 0u64;

            while Instant::now() < worker_deadline {
                let plan = match mode {
                    BenchMode::Put => {
                        let mut writes = Vec::with_capacity(txn_ops);
                        for _ in 0..txn_ops {
                            let key = key_for(rng.gen_range(0..keyspace));
                            let value = value_for(((worker as u64) << 32) ^ write_seq, value_size);
                            write_seq = write_seq.wrapping_add(1);
                            writes.push((key, value));
                        }
                        TxnPlan::Put(writes)
                    }
                    BenchMode::Get => {
                        let mut keys = Vec::with_capacity(txn_ops);
                        for _ in 0..txn_ops {
                            keys.push(key_for(rng.gen_range(0..keyspace)));
                        }
                        TxnPlan::Get(keys)
                    }
                    BenchMode::Scan => {
                        let mut ranges = Vec::with_capacity(txn_ops);
                        for _ in 0..txn_ops {
                            let start_id = rng.gen_range(0..keyspace);
                            let (range_start, range_end) =
                                scan_bounds(start_id, keyspace, scan_len);
                            ranges.push((range_start, range_end, scan_len));
                        }
                        TxnPlan::Scan(ranges)
                    }
                };

                let latency = match (&backend, plan) {
                    (SharedBackend::ScaleKv(compute), TxnPlan::Put(writes)) => {
                        do_scale_put_txn(
                            compute,
                            &writes,
                            worker_deadline,
                            &mut stats.retryable_write_errors,
                        )
                        .await?
                    }
                    (SharedBackend::ScaleKv(compute), TxnPlan::Get(keys)) => {
                        do_scale_get_txn(
                            compute,
                            &keys,
                            worker_deadline,
                            &mut stats.retryable_read_errors,
                        )
                        .await?
                    }
                    (SharedBackend::ScaleKv(compute), TxnPlan::Scan(ranges)) => {
                        do_scale_scan_txn(
                            compute,
                            &ranges,
                            worker_deadline,
                            &mut stats.retryable_read_errors,
                        )
                        .await?
                    }
                    (SharedBackend::Redb(db), plan) => {
                        Some(do_redb_txn(Arc::clone(db), plan).await?)
                    }
                };

                let Some(latency) = latency else {
                    break;
                };

                match mode {
                    BenchMode::Put => {
                        stats.writes += 1;
                    }
                    BenchMode::Get | BenchMode::Scan => {
                        stats.reads += 1;
                    }
                }
                reservoir.record(latency.as_micros() as u64, &mut rng);
                txn_count += 1;

                if txn_count % 64 == 0 {
                    tokio::task::yield_now().await;
                }
            }

            stats.samples = reservoir.samples;
            Ok::<WorkerStats, anyhow::Error>(stats)
        }));
    }

    let mut out = WorkerStats::default();
    for handle in join_handles {
        let stats = handle.await??;
        out.reads += stats.reads;
        out.writes += stats.writes;
        out.retryable_read_errors += stats.retryable_read_errors;
        out.retryable_write_errors += stats.retryable_write_errors;
        out.samples.extend(stats.samples);
    }

    Ok(out)
}

fn backend_name(backend: BackendKind) -> &'static str {
    match backend {
        BackendKind::ScaleKv => "scale-kv",
        BackendKind::Redb => "redb",
    }
}

async fn run(cfg: Config, local: &LocalSet) -> anyhow::Result<()> {
    let (_state, backend) = init_backend(&cfg, local).await?;

    let preload_start = Instant::now();
    if cfg.preload {
        preload(&backend, &cfg).await?;
    }
    let preload_elapsed = preload_start.elapsed();
    let workload_keyspace = effective_keyspace(&cfg);

    if cfg.preload_only {
        println!("backend={}", backend_name(cfg.backend));
        println!(
            "config mode={} clients={} duration_secs={} keyspace={} preload_keys={} preload={} preload_only={} allow_misses={} value_size={} txn_ops={} scan_len={} seed={}",
            mode_name(cfg.mode),
            cfg.clients,
            cfg.duration_secs,
            cfg.keyspace,
            cfg.preload_keys,
            cfg.preload,
            cfg.preload_only,
            cfg.allow_misses,
            cfg.value_size,
            cfg.txn_ops,
            cfg.scan_len,
            cfg.seed,
        );
        println!("workload_keyspace={}", workload_keyspace);
        println!("preload_seconds={:.3}", preload_elapsed.as_secs_f64());
        println!("preload_only=true");
        return Ok(());
    }

    let run_start = Instant::now();
    let mut stats = run_workers(backend, &cfg).await?;
    let run_elapsed = run_start.elapsed();

    let total_ops = stats.reads + stats.writes;
    let throughput = if run_elapsed.as_secs_f64() > 0.0 {
        total_ops as f64 / run_elapsed.as_secs_f64()
    } else {
        0.0
    };

    let p50 = percentile_us(&mut stats.samples, 0.50);
    let p95 = percentile_us(&mut stats.samples, 0.95);
    let p99 = percentile_us(&mut stats.samples, 0.99);

    println!("backend={}", backend_name(cfg.backend));
    println!(
        "config mode={} clients={} duration_secs={} keyspace={} preload_keys={} preload={} preload_only={} allow_misses={} value_size={} txn_ops={} scan_len={} seed={}",
        mode_name(cfg.mode),
        cfg.clients,
        cfg.duration_secs,
        cfg.keyspace,
        cfg.preload_keys,
        cfg.preload,
        cfg.preload_only,
        cfg.allow_misses,
        cfg.value_size,
        cfg.txn_ops,
        cfg.scan_len,
        cfg.seed,
    );
    println!("workload_keyspace={}", workload_keyspace);
    println!("preload_seconds={:.3}", preload_elapsed.as_secs_f64());
    println!("run_seconds={:.3}", run_elapsed.as_secs_f64());
    println!("ops_total={}", total_ops);
    println!("reads={}", stats.reads);
    println!("writes={}", stats.writes);
    println!("throughput_ops_per_sec={:.2}", throughput);
    println!("latency_p50_us={}", p50);
    println!("latency_p95_us={}", p95);
    println!("latency_p99_us={}", p99);
    if matches!(cfg.backend, BackendKind::ScaleKv) {
        println!("retryable_read_errors={}", stats.retryable_read_errors);
        println!("retryable_write_errors={}", stats.retryable_write_errors);
    }

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let cfg = parse_config()?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?;

    rt.block_on(async {
        let local = LocalSet::new();
        local.run_until(run(cfg, &local)).await
    })
}
