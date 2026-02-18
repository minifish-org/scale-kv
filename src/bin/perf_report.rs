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

fn parse_rows(content: &str) -> anyhow::Result<Vec<Row>> {
    let mut rows = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        rows.push(serde_json::from_str::<Row>(line)?);
    }
    Ok(rows)
}

fn render_report(mut rows: Vec<Row>, input: &str) -> anyhow::Result<String> {
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

    Ok(report)
}

fn build_report_from_args(args: &[String]) -> anyhow::Result<(String, Option<String>)> {
    let input = parse_arg(&args, "--in")
        .ok_or_else(|| anyhow::anyhow!("missing required --in <jsonl_path>"))?;
    let output = parse_arg(&args, "--out");

    let content = fs::read_to_string(&input)?;
    let rows = parse_rows(&content)?;
    let report = render_report(rows, &input)?;
    Ok((report, output))
}

fn write_report(report: &str, output: Option<String>) -> anyhow::Result<()> {
    if let Some(path) = output {
        fs::write(&path, report)?;
        eprintln!("wrote report: {}", path);
    } else {
        let mut out = io::stdout().lock();
        out.write_all(report.as_bytes())?;
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let (report, output) = build_report_from_args(&args)?;
    write_report(&report, output)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row_line(read_ratio: u32, concurrency: usize, throughput: f64) -> String {
        format!(
            "{{\"records\":1000,\"ops\":5000,\"concurrency\":{},\"read_ratio\":{},\"throughput_ops_per_sec\":{},\"p50_us\":10,\"p95_us\":20,\"p99_us\":30,\"read_ops\":1,\"write_ops\":2}}",
            concurrency, read_ratio, throughput
        )
    }

    #[test]
    fn test_parse_arg_and_rows() {
        let args = vec![
            "perf_report".to_string(),
            "--in".to_string(),
            "in.jsonl".to_string(),
        ];
        assert_eq!(parse_arg(&args, "--in").as_deref(), Some("in.jsonl"));
        assert!(parse_arg(&args, "--out").is_none());

        let content = format!(
            "# comment\n{}\n\n{}\n",
            row_line(80, 4, 2000.0),
            row_line(50, 2, 1000.0)
        );
        let rows = parse_rows(&content).unwrap();
        assert_eq!(rows.len(), 2);

        let dangling = vec!["perf_report".to_string(), "--in".to_string()];
        assert!(parse_arg(&dangling, "--in").is_none());
    }

    #[test]
    fn test_render_report_orders_rows_and_has_best_table() {
        let rows = vec![
            serde_json::from_str::<Row>(&row_line(80, 8, 3000.0)).unwrap(),
            serde_json::from_str::<Row>(&row_line(80, 4, 3500.0)).unwrap(),
            serde_json::from_str::<Row>(&row_line(20, 2, 500.0)).unwrap(),
        ];
        let report = render_report(rows, "x.jsonl").unwrap();
        assert!(report.contains("# Performance Baseline Report"));
        assert!(report.contains("| 80 | 4 | 3500.00 |"));
        assert!(report.contains("| 20 | 2 | 500.00 |"));
    }

    #[test]
    fn test_render_report_rejects_empty() {
        assert!(render_report(Vec::new(), "empty.jsonl").is_err());
    }

    #[test]
    fn test_parse_rows_rejects_invalid_json() {
        let err = parse_rows("{bad-json").unwrap_err().to_string();
        assert!(!err.is_empty());
    }

    #[test]
    fn test_build_and_write_report_from_args() {
        let dir = tempfile::tempdir().unwrap();
        let in_path = dir.path().join("in.jsonl");
        let out_path = dir.path().join("out.md");
        let content = format!("{}\n", row_line(80, 4, 2000.0));
        fs::write(&in_path, content).unwrap();

        let args = vec![
            "perf_report".to_string(),
            "--in".to_string(),
            in_path.display().to_string(),
            "--out".to_string(),
            out_path.display().to_string(),
        ];
        let (report, output) = build_report_from_args(&args).unwrap();
        write_report(&report, output).unwrap();
        let written = fs::read_to_string(out_path).unwrap();
        assert!(written.contains("Performance Baseline Report"));
    }
}
