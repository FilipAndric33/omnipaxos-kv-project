#!/bin/bash
# Benchmark Performance vs Clock Quality: runs 3 experiments (high/medium/low)
# and collects consensus latency, throughput, and fast-path ratio.
# Run from build_scripts: ./clock_quality_benchmark.sh

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RUST_LOG="${RUST_LOG:-info}"
LOG_DIR="./logs"
METRICS_DIR="./clock_bench_results"
mkdir -p "$LOG_DIR" "$METRICS_DIR"

# Kill any leftover server processes so ports 8001-8003 are free
kill_servers() {
    pkill -f "target/debug/server" 2>/dev/null || true
    pkill -f "target/release/server" 2>/dev/null || true
    sleep 2
}
kill_servers

# Build once
echo "Building server and client..."
cargo build --manifest-path="../Cargo.toml" --bin server --bin client

run_experiment() {
    local quality="$1"
    local uncertainty_us="$2"
    local sync_ms="$3"
    echo "===== Clock quality: $quality (±${uncertainty_us}μs uncertainty, ${sync_ms}ms sync) ====="

    export OMNIPAXOS_CLOCK_UNCERTAINTY_US="$uncertainty_us"
    export OMNIPAXOS_CLOCK_SYNC_INTERVAL_MS="$sync_ms"

    # Use /tmp under WSL to avoid permission issues (server + client output)
    if [ -w /tmp ] 2>/dev/null; then
        OUT_DIR="/tmp/omnipaxos-clock-bench-$quality"
    else
        OUT_DIR="$METRICS_DIR/out-$quality"
    fi
    mkdir -p "$OUT_DIR"

    # Benchmark server configs: server 1 has 2 clients (both connect to leader), servers 2/3 have 0
    for i in 1 2 3; do
        sed -e "s|output_filepath = .*|output_filepath = \"$OUT_DIR/server-${i}.json\"|" \
            -e "s|num_clients = .*|num_clients = $([ "$i" = 1 ] && echo 2 || echo 0)|" \
            "./server-${i}-config.toml" > "$OUT_DIR/server-${i}-config.toml"
    done

    # Start 3 servers in background (stagger slightly so each binds before next)
    for i in 1 2 3; do
        RUST_LOG=$RUST_LOG SERVER_CONFIG_FILE="$OUT_DIR/server-${i}-config.toml" CLUSTER_CONFIG_FILE="./cluster-config.toml" \
        ../target/debug/server &
        sleep 0.5
    done
    sleep 3

    # Client configs: output to OUT_DIR (WSL writable), and both clients connect to leader (server 1)
    sed "s|\./logs|$OUT_DIR|g" ./client-1-config.toml > "$OUT_DIR/client-1-config.toml"
    sed -e "s|\./logs|$OUT_DIR|g" -e "s|server_id = 2|server_id = 1|" -e "s|127.0.0.1:8002|127.0.0.1:8001|" \
        ./client-2-config.toml > "$OUT_DIR/client-2-config.toml"

    RUST_LOG=$RUST_LOG CONFIG_FILE="$OUT_DIR/client-1-config.toml" ../target/debug/client &
    CLIENT1_PID=$!
    RUST_LOG=$RUST_LOG CONFIG_FILE="$OUT_DIR/client-2-config.toml" ../target/debug/client
    wait $CLIENT1_PID 2>/dev/null || true

    # Copy client and server metrics to results dir
    cp -f "$OUT_DIR/client-1.csv" "$METRICS_DIR/client-1-$quality.csv" 2>/dev/null || true
    cp -f "$OUT_DIR/client-2.csv" "$METRICS_DIR/client-2-$quality.csv" 2>/dev/null || true
    for i in 1 2 3; do
        [ -f "$OUT_DIR/server-$i.metrics.json" ] && cp "$OUT_DIR/server-$i.metrics.json" "$METRICS_DIR/server-$quality-$i.metrics.json"
    done

    kill_servers
}

# High: ±10μs, 1ms sync
run_experiment "high" 10 1
sleep 3
# Medium: ±100μs, 10ms sync
run_experiment "medium" 100 10
sleep 3
# Low: ±1ms, 100ms sync
run_experiment "low" 1000 100

echo ""
echo "Results written to $METRICS_DIR. Run: python3 compute_clock_bench_metrics.py $METRICS_DIR"
if command -v python3 &>/dev/null; then
    python3 compute_clock_bench_metrics.py "$METRICS_DIR" 2>/dev/null || echo "Add compute_clock_bench_metrics.py to print summary table."
fi
