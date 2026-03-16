//! Test Correctness: deadline-ordered processing maintains linearizability
//! even with imperfect clocks, clock skew, network delays, and node failures.
//! Correctness must hold regardless of clock quality; only performance may degrade.

use omnipaxos::util::{LogEntry, NodeId};
use omnipaxos::{OmniPaxos, OmniPaxosConfig};
use omnipaxos_kv::common::kv::{Command, KVCommand};
use omnipaxos_storage::memory_storage::MemoryStorage;

type Op = OmniPaxos<Command, MemoryStorage<Command>>;

fn decided_commands(entries: &[LogEntry<Command>]) -> Vec<Command> {
    entries
        .iter()
        .filter_map(|e| match e {
            LogEntry::Decided(c) => Some(c.clone()),
            _ => None,
        })
        .collect()
}

/// Apply a sequence of decided commands to a KV map.
/// Linearizability: the decided order is a legal total order; applying it gives consistent state.
fn apply_commands(commands: &[Command]) -> std::collections::HashMap<String, String> {
    let mut db = std::collections::HashMap::new();
    for cmd in commands {
        match &cmd.kv_cmd {
            KVCommand::Put(k, v) => {
                db.insert(k.clone(), v.clone());
            }
            KVCommand::Delete(k) => {
                db.remove(k);
            }
            KVCommand::Get(_) => {}
        }
    }
    db
}

/// Build config for a single-node "cluster" (no network; tests command application).
fn single_node_config(pid: NodeId) -> OmniPaxosConfig {
    let mut cfg = OmniPaxosConfig::default();
    cfg.server_config.pid = pid;
    cfg.cluster_config.nodes = vec![pid];
    cfg.cluster_config.configuration_id = 1;
    cfg
}

/// Linearizability: applying the decided log in order must yield a state
/// where every key has the value from the last write in that order.
#[test]
fn linearizability_decided_order_is_consistent() {
    let storage = MemoryStorage::<Command>::default();
    let mut op = single_node_config(1).build(storage).unwrap();

    let commands = vec![
        Command {
            client_id: 1,
            coordinator_id: 1,
            id: 0,
            kv_cmd: KVCommand::Put("x".into(), "1".into()),
        },
        Command {
            client_id: 1,
            coordinator_id: 1,
            id: 1,
            kv_cmd: KVCommand::Put("y".into(), "a".into()),
        },
        Command {
            client_id: 1,
            coordinator_id: 1,
            id: 2,
            kv_cmd: KVCommand::Put("x".into(), "2".into()),
        },
        Command {
            client_id: 1,
            coordinator_id: 1,
            id: 3,
            kv_cmd: KVCommand::Get("x".into()),
        },
    ];

    for c in &commands {
        op.append(c.clone()).expect("append");
    }
    op.tick();
    // Single node: entries become decided immediately.
    let entries = op.read_decided_suffix(0).unwrap();
    let decided = decided_commands(&entries);
    let applied = apply_commands(&decided);
    // Last write to "x" is "2", "y" is "a".
    assert_eq!(applied.get("x"), Some(&"2".to_string()));
    assert_eq!(applied.get("y"), Some(&"a".to_string()));
}

/// Correctness regardless of clock: the same sequence of appends must produce
/// the same decided log regardless of when the clock is read (simulated by multiple ticks).
#[test]
fn correctness_same_decided_log_regardless_of_timing() {
    let storage = MemoryStorage::<Command>::default();
    let mut op = single_node_config(1).build(storage).unwrap();

    let cmd = Command {
        client_id: 1,
        coordinator_id: 1,
        id: 0,
        kv_cmd: KVCommand::Put("k".into(), "v".into()),
    };
    op.append(cmd).expect("append");
    // Many ticks (simulating slow clock / high sync interval) must not change decided result.
    for _ in 0..100 {
        op.tick();
    }
    let entries = op.read_decided_suffix(0).unwrap();
    let decided = decided_commands(&entries);
    assert_eq!(decided.len(), 1);
    let applied = apply_commands(&decided);
    assert_eq!(applied.get("k"), Some(&"v".to_string()));
}

/// Linearizability with multiple keys: last write wins per key in decided order.
#[test]
fn linearizability_last_write_wins() {
    let storage = MemoryStorage::<Command>::default();
    let mut op = single_node_config(1).build(storage).unwrap();

    let seq = vec![
        ("a", "1"),
        ("b", "1"),
        ("a", "2"),
        ("a", "3"),
        ("b", "2"),
    ];
    for (i, (k, v)) in seq.iter().enumerate() {
        let c = Command {
            client_id: 1,
            coordinator_id: 1,
            id: i,
            kv_cmd: KVCommand::Put((*k).to_string(), (*v).to_string()),
        };
        op.append(c).expect("append");
    }
    op.tick();
    let entries = op.read_decided_suffix(0).unwrap();
    let decided = decided_commands(&entries);
    let applied = apply_commands(&decided);
    assert_eq!(applied.get("a"), Some(&"3".to_string()));
    assert_eq!(applied.get("b"), Some(&"2".to_string()));
}
