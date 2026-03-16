#!/bin/bash

usage="Usage: run-local-cluster.sh"
cluster_size=3
rust_log="info"

# Clean up child processes
interrupt() {
    pkill -P $$
}
trap "interrupt" SIGINT

# Servers' output is saved into logs dir.
# Use /tmp when running under WSL to avoid Permission denied on Windows mount (/mnt/c/...).
if [ -w /tmp ] 2>/dev/null; then
    local_experiment_dir="/tmp/omnipaxos-kv-logs"
else
    local_experiment_dir="./logs"
fi
mkdir -p "${local_experiment_dir}"

# Run servers (pre-build so all start listening before connecting)
cargo build --manifest-path="../Cargo.toml" --bin server
cluster_config_path="./cluster-config.toml"
for ((i = 1; i <= cluster_size; i++)); do
    server_config_path="./server-${i}-config.toml"
    RUST_LOG=$rust_log SERVER_CONFIG_FILE=$server_config_path CLUSTER_CONFIG_FILE=$cluster_config_path \
    OMNIPAXOS_OUTPUT_FILEPATH="${local_experiment_dir}/server-${i}.json" \
    ../target/debug/server &
done
wait

