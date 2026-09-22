# scale-kv

An experimental Rust key-value store exploring separate compute and storage,
page-based B+Trees, write-ahead logging, MVCC and quorum-backed transactions.
It is a research project, not a production database. APIs and on-disk formats
may change without migration support. Do not use it for irreplaceable data.

## Requirements

- Rust stable with Rust 2024 edition support (1.85 or newer); CI uses stable.
- Cap'n Proto compiler (`capnp`) on PATH.
- macOS or Linux; tests use local temporary files and loopback networking.

Install Cap'n Proto with `brew install capnp` on macOS or
`sudo apt-get install capnproto` on Debian/Ubuntu.

## Build and test

```sh
git clone https://github.com/minifish-org/scale-kv.git
cd scale-kv
./scripts/check.sh
cargo build --locked --bins
```

The check script validates formatting and runs library, binary and integration
tests with warnings treated as errors. The schema is generated during the build.

## Run a local experiment

Start a storage process in one terminal:

```sh
cargo run --bin storage_server -- --addr 127.0.0.1:50051 --dir ./data/demo
```

In another terminal, run a small mixed workload:

```sh
cargo run --bin workload_bench -- --addr 127.0.0.1:50051 --records 1000 --ops 2000 --concurrency 4 --read-ratio 80
```

The workload prints JSON throughput and latency statistics. Use disposable data;
stop the server with Ctrl-C. The RPC service has no public-internet authentication
or TLS layer. Keep it on loopback or an isolated test network.

## Architecture and evidence

- [Design](distributed-kv-store-design.md): compute/storage interfaces and design history.
- [API draft](docs/api-v1-draft.md): embedded transactions and storage RPC.
- [Recovery drills](docs/fault-drills.md): fault and recovery scenarios.
- [Reproducible benchmarks](docs/performance-baseline.md): workload matrix and report generation.

Design documents and historical benchmark snapshots describe specific revisions
and environments. They are not promises of production durability, fault tolerance
or universal performance. Check the tests and implementation when evaluating a feature.

## Contributing and security

See [CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md).

## License

The project as a whole uses [AGPL-3.0-only](LICENSE). Existing source files with
an explicit Apache-2.0 SPDX header retain that license; its text is included in
[LICENSES/Apache-2.0.txt](LICENSES/Apache-2.0.txt). Dependencies retain their
upstream licenses. Copyright and attribution notices must be preserved.
