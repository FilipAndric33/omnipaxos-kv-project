#!/usr/bin/env python3
"""
Read clock-quality benchmark results and print:
- Consensus latency (mean response_time - request_time in ms)
- Throughput (completed ops per second)
- Fast-path ratio (from server metrics: % of responses not requiring leader/clock intervention)
"""
import csv
import json
import os
import sys
from pathlib import Path


def load_client_csv(path: Path) -> list[dict]:
    rows = []
    with open(path, newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    return rows


def client_metrics(rows: list[dict]) -> tuple[float, float]:
    """Returns (mean_latency_ms, throughput_ops_per_sec)."""
    if not rows:
        return 0.0, 0.0
    latencies = []
    for r in rows:
        rt = r.get("response_time") or r.get("response time")
        qt = r.get("request_time") or r.get("request time")
        if rt and qt:
            try:
                latencies.append(float(rt) - float(qt))
            except ValueError:
                pass
    if not latencies:
        return 0.0, 0.0
    mean_latency = sum(latencies) / len(latencies)
    completed = len(latencies)
    times = []
    for r in rows:
        for k in ("request_time", "request time", "response_time", "response time"):
            if r.get(k):
                try:
                    times.append(float(r[k]))
                    break
                except ValueError:
                    pass
    duration_sec = (max(times) - min(times)) / 1000.0 if len(times) >= 2 else 1.0
    throughput = completed / duration_sec if duration_sec > 0 else 0.0
    return mean_latency, throughput


def server_fast_path_ratio(metrics_dir: Path, quality: str) -> float:
    """Average fast_path_ratio across server metrics files for this quality."""
    total_responses = 0
    fast_path = 0
    for i in (1, 2, 3):
        p = metrics_dir / f"server-{quality}-{i}.metrics.json"
        if not p.exists():
            continue
        with open(p) as f:
            m = json.load(f)
        total_responses += m.get("total_responses", 0)
        fast_path += m.get("fast_path_responses", 0)
    if total_responses == 0:
        return 0.0
    return 100.0 * fast_path / total_responses


def main() -> None:
    if len(sys.argv) < 2:
        metrics_dir = Path(__file__).parent / "clock_bench_results"
    else:
        metrics_dir = Path(sys.argv[1])
    if not metrics_dir.is_dir():
        print(f"Directory not found: {metrics_dir}")
        sys.exit(1)

    print("Clock Quality Benchmark Results")
    print("=" * 70)
    print(f"{'Quality':<10} {'Latency (ms)':<14} {'Throughput (ops/s)':<22} {'Fast-path %':<12}")
    print("-" * 70)

    for quality in ("high", "medium", "low"):
        all_latencies = []
        all_throughputs = []
        for c in (1, 2):
            csv_path = metrics_dir / f"client-{c}-{quality}.csv"
            if not csv_path.exists():
                continue
            rows = load_client_csv(csv_path)
            lat, thr = client_metrics(rows)
            all_latencies.append(lat)
            all_throughputs.append(thr)
        mean_lat = sum(all_latencies) / len(all_latencies) if all_latencies else 0
        total_thr = sum(all_throughputs)
        fp = server_fast_path_ratio(metrics_dir, quality)
        print(f"{quality:<10} {mean_lat:<14.2f} {total_thr:<22.1f} {fp:<12.1f}")

    print("=" * 70)
    print("High: ±10μs uncertainty, 1ms sync  |  Medium: ±100μs, 10ms sync  |  Low: ±1ms, 100ms sync")


if __name__ == "__main__":
    main()
