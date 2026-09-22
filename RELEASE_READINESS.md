# Public release preparation — 2026-09-22

## Validation

- `./scripts/check.sh`: formatting and all 151 tests passed with warnings treated as errors.
- Cap'n Proto and LRU were upgraded to address dependency advisories; RPC handlers were adapted to the current generated trait. Existing transaction, recovery and quorum integration tests passed.
- Unused sled/redb dependencies were removed rather than shipping their unused dependency trees.
- `cargo audit`: no known vulnerabilities after the lockfile update.
- Gitleaks found no secrets in the fetched local Git history and staged release changes.

## Limits

This is an experimental storage implementation. These checks do not certify
production durability, distributed consensus, throughput, or safety under all
faults. Historical benchmark documents are not rerun performance claims.

The project-wide AGPL license and original per-file Apache notices are documented
in README.md. See CONTRIBUTING.md and SECURITY.md for participation and reporting.
