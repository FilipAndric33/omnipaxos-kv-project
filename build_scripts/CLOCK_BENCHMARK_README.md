# Clock Quality Benchmark & Correctness

## Benchmark: Performance vs Clock Quality

Run experiments with three clock quality configurations:

| Quality | Uncertainty | Sync interval |
|---------|-------------|---------------|
| **High**   | ±10μs  | 1 ms  |
| **Medium** | ±100μs | 10 ms |
| **Low**    | ±1 ms  | 100 ms |

For each configuration the benchmark measures:

- **Consensus latency** (ms): mean client request → response time from client CSV.
- **Throughput** (ops/s): completed operations per second.
- **Fast-path ratio** (%): percentage of responses not requiring leader/clock intervention (from server metrics).

### How to run (WSL or Linux)

```bash
cd build_scripts
chmod +x clock_quality_benchmark.sh
./clock_quality_benchmark.sh
```

Then print the summary table:

```bash
python3 compute_clock_bench_metrics.py clock_bench_results
```

Results are written to `clock_bench_results/` (client CSVs and server `*.metrics.json`).

### Config via TOML or env

- **TOML**: add a `[clock]` section to server config:
  ```toml
  [clock]
  clock_uncertainty_us = 10
  clock_sync_interval_ms = 1
  ```
- **Env** (used by the benchmark script): set `OMNIPAXOS_CLOCK_UNCERTAINTY_US` and `OMNIPAXOS_CLOCK_SYNC_INTERVAL_MS`.

---

## Test Correctness: Linearizability

Tests demonstrate that deadline-ordered processing maintains **linearizability** regardless of clock quality; only performance may degrade.

### Run tests

```bash
cargo test linearizability
cargo test correctness_same_decided
cargo test last_write_wins
```

### Test cases

1. **linearizability_decided_order_is_consistent**  
   Applying the decided log in order yields a consistent KV state (last write per key).

2. **correctness_same_decided_log_regardless_of_timing**  
   The same append sequence produces the same decided log even with many ticks (simulating slow clock / high sync interval).

3. **linearizability_last_write_wins**  
   Multiple writes to the same key; applying the decided order gives last-write-wins semantics.

Correctness holds regardless of imperfect clocks, clock skew, network delays, and node failures because:

- Consensus (OmniPaxos) fixes a total order of commands; the clock does not change that order.
- The server applies the **decided** suffix in order to the database; clock only affects when resync happens (performance), not what gets decided.
