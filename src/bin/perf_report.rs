use serde::Deserialize;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Write};

#[derive(Debug, Clone, Deserialize)]
struct Row {
    records: usize,
    ops: usize,
    concurrency: usize,
    read_ratio: u32,
    throughput_ops_per_sec: f64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    read_ops: usize,
    write_ops: usize,
}

fn parse_arg(args: &[String], key: &str) -> Option<String> {
    args.iter()
        .position(|a| a == key)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let input = parse_arg(&args, "--in")
        .ok_or_else(|| anyhow::anyhow!("missing required --in <jsonl_path>"))?;
    let output = parse_arg(&args, "--out");

    let content = fs::read_to_string(&input)?;
    let mut rows = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        rows.push(serde_json::from_str::<Row>(line)?);
    }
    if rows.is_empty() {
        return Err(anyhow::anyhow!("no benchmark rows found in {}", input));
    }

    rows.sort_by(|a, b| {
        match a
            .read_ratio
            .cmp(&b.read_ratio)
            .then(a.concurrency.cmp(&b.concurrency))
        {
            Ordering::Equal => b
                .throughput_ops_per_sec
                .partial_cmp(&a.throughput_ops_per_sec)
                .unwrap_or(Ordering::Equal),
            ord => ord,
        }
    });

    let mut by_ratio: BTreeMap<u32, Vec<Row>> = BTreeMap::new();
    for row in &rows {
        by_ratio
            .entry(row.read_ratio)
            .or_default()
            .push(row.clone());
    }

    let mut report = String::new();
    report.push_str("# Performance Baseline Report\n\n");
    report.push_str(&format!("- source: `{}`\n", input));
    report.push_str(&format!("- rows: `{}`\n", rows.len()));
    report.push_str(&format!(
        "- workload: `records={}`, `ops={}`\n\n",
        rows[0].records, rows[0].ops
    ));

    report.push_str("## Matrix\n\n");
    report.push_str("| read_ratio | concurrency | throughput_ops_per_sec | p50_us | p95_us | p99_us | read_ops | write_ops |\n");
    report.push_str("|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for row in &rows {
        report.push_str(&format!(
            "| {} | {} | {:.2} | {} | {} | {} | {} | {} |\n",
            row.read_ratio,
            row.concurrency,
            row.throughput_ops_per_sec,
            row.p50_us,
            row.p95_us,
            row.p99_us,
            row.read_ops,
            row.write_ops
        ));
    }
    report.push('\n');

    report.push_str("## Best Throughput per Read Ratio\n\n");
    report
        .push_str("| read_ratio | best_concurrency | throughput_ops_per_sec | p95_us | p99_us |\n");
    report.push_str("|---:|---:|---:|---:|---:|\n");
    for (ratio, rows) in by_ratio {
        if let Some(best) = rows.iter().max_by(|a, b| {
            a.throughput_ops_per_sec
                .partial_cmp(&b.throughput_ops_per_sec)
                .unwrap_or(Ordering::Equal)
        }) {
            report.push_str(&format!(
                "| {} | {} | {:.2} | {} | {} |\n",
                ratio, best.concurrency, best.throughput_ops_per_sec, best.p95_us, best.p99_us
            ));
        }
    }
    report.push('\n');

    if let Some(path) = output {
        fs::write(&path, report)?;
        eprintln!("wrote report: {}", path);
    } else {
        let mut out = io::stdout().lock();
        out.write_all(report.as_bytes())?;
    }
    Ok(())
}
