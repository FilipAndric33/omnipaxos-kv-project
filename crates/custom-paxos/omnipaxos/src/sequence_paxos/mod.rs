use log::warn;
use tokio::sync::Mutex;

use super::{ballot_leader_election::Ballot, messages::sequence_paxos::*, util::LeaderState};
use crate::{
    ClusterConfig, OmniPaxosConfig, ProposeErr,
    buffers::{Buffers, BuffersRef},
    clock::Clock,
    messages::Message,
    storage::{
        Entry, Snapshot, StopSign, Storage,
        internal_storage::{InternalStorage, InternalStorageConfig},
    },
    util::{FlexibleQuorum, LogSync, NodeId, Quorum, READ_ERROR_MSG, WRITE_ERROR_MSG},
};
use std::{fmt::Debug, sync::Arc};

pub mod follower;
pub mod leader;

/// a Sequence Paxos replica. Maintains local state of the replicated log, handles incoming messages and produces outgoing messages that the user has to fetch periodically and send using a network implementation.
/// User also has to periodically fetch the decided entries that are guaranteed to be strongly consistent and linearizable, and therefore also safe to be used in the higher level application.
/// If snapshots are not desired to be used, use `()` for the type parameter `S`.
#[allow(dead_code)]
pub struct SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    pub internal_storage: Arc<Mutex<InternalStorage<B, T>>>,
    buffers: BuffersRef<T>,
    clock: Clock,
    pid: NodeId,
    peers: Vec<NodeId>, // excluding self pid
    state: (Role, Phase),
    outgoing: Arc<Mutex<Vec<Message<T>>>>,
    leader_state: LeaderState,
    latest_accepted_meta: Option<(Ballot, usize)>,
}

impl<T, B> SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    /*** User functions ***/
    /// Creates a Sequence Paxos replica.
    pub(crate) async fn with(config: SequencePaxosConfig, storage: B, clock: Clock) -> Self {
        let pid = config.pid;
        let peers = config.peers;
        let num_nodes = &peers.len() + 1;
        let quorum = Quorum::with(config.flexible_quorum, num_nodes);
        let max_peer_pid = peers.iter().max().unwrap();
        let max_pid = *std::cmp::max(max_peer_pid, &pid) as usize;
        let outgoing = Vec::with_capacity(config.buffer_size);
        let (state, leader) = match storage
            .get_promise()
            .expect("storage error while trying to read promise")
        {
            // if we recover a promise from storage then we must do failure recovery
            Some(b) => {
                // TODO: Nazha recovering
                let state = (Role::Follower, Phase::Recover);
                (state, b)
            }
            None => ((Role::Follower, Phase::None), Ballot::default()),
        };
        let internal_storage_config = InternalStorageConfig {
            batch_size: config.batch_size,
        };
        let paxos = SequencePaxos {
            internal_storage: Arc::new(Mutex::new(InternalStorage::with(
                storage,
                internal_storage_config,
            ))),
            buffers: Buffers::new(clock.clone()),
            clock,
            pid,
            peers,
            state,
            outgoing: Arc::new(Mutex::new(outgoing)),
            leader_state: LeaderState::with(leader, max_pid, quorum),
            latest_accepted_meta: None,
        };
        paxos
            .internal_storage
            .lock()
            .await
            .set_promise(leader)
            .expect(WRITE_ERROR_MSG);

        paxos
    }

    pub(crate) fn get_state(&self) -> &(Role, Phase) {
        &self.state
    }

    pub(crate) async fn get_promise(&self) -> Ballot {
        self.internal_storage.lock().await.get_promise()
    }

    /// Moves the outgoing messages from this replica into the buffer. The messages should then be sent via the network implementation.
    /// If `buffer` is empty, it gets swapped with the internal message buffer. Otherwise, messages are appended to the buffer. This prevents messages from getting discarded.
    /// the buffer.
    pub(crate) async fn take_outgoing_msgs(&mut self, buffer: &mut Vec<Message<T>>) {
        if buffer.is_empty() {
            let mut locked = self.outgoing.lock().await;
            std::mem::swap(buffer, &mut locked);
        } else {
            // User has unsent messages in their buffer, must extend their buffer.
            let mut locked = self.outgoing.lock().await;
            buffer.append(&mut locked);
        }
        self.latest_accepted_meta = None;
    }

    /// Handle an incoming message.
    pub(crate) async fn handle<E: Fn(T) -> Option<Option<String>>>(
        &mut self,
        m: PaxosMessage<T>,
        e: E,
    ) {
        match m.msg {
            PaxosMsg::Ack(_, _, _, _) => warn!("should not receiv that."),
            PaxosMsg::Confirm(c, accepted_idx) => self.handle_execution(c, accepted_idx, e).await,
        }
    }

    /// Returns whether this Sequence Paxos has been reconfigured
    pub(crate) async fn is_reconfigured(&self) -> Option<StopSign> {
        let locked = self.internal_storage.lock().await;
        match locked.get_stopsign() {
            Some(ss) if locked.stopsign_is_decided() => Some(ss),
            _ => None,
        }
    }

    /// Returns whether this Sequence Paxos instance is stopped, i.e. if it has been reconfigured.
    async fn accepted_reconfiguration(&self) -> bool {
        self.internal_storage.lock().await.get_stopsign().is_some()
    }

    /// Append an entry to the replicated log.
    pub(crate) async fn append<S: Fn(T) -> Option<Option<String>>>(
        &mut self,
        proxy: NodeId,
        entry: T,
        s: S,
    ) -> Result<(), ProposeErr<T>> {
        if self.accepted_reconfiguration().await {
            Err(ProposeErr::PendingReconfigEntry(entry))
        } else {
            self.propose_entry(proxy, entry, s);
            Ok(())
        }
    }

    /// Propose a reconfiguration. Returns an error if already stopped or `new_config` is invalid.
    /// `new_config` defines the cluster-wide configuration settings for the next cluster.
    /// `metadata` is optional data to commit alongside the reconfiguration.
    pub(crate) async fn reconfigure(
        &mut self,
        new_config: ClusterConfig,
        metadata: Option<Vec<u8>>,
    ) -> Result<(), ProposeErr<T>> {
        if self.accepted_reconfiguration().await {
            return Err(ProposeErr::PendingReconfigConfig(new_config, metadata));
        }
        match self.state {
            _ => {}
        }
        Ok(())
    }

    async fn get_current_leader(&self) -> NodeId {
        self.get_promise().await.pid
    }

    /// Handles re-establishing a connection to a previously disconnected peer.
    /// This should only be called if the underlying network implementation indicates that a connection has been re-established.
    pub(crate) async fn reconnected(&mut self, pid: NodeId) {
        if pid == self.pid {
            return;
        } else if pid == self.get_current_leader().await {
            self.state = (Role::Follower, Phase::Recover);
        }
    }

    fn propose_entry<S: Fn(T) -> Option<Option<String>>>(&mut self, proxy: NodeId, entry: T, s: S) {
        match self.state {
            (Role::Follower, _) => self.handle_new_proposal(proxy, entry, false, s),
            (Role::Leader, _) => self.handle_new_proposal(proxy, entry, true, s),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn get_leader_state(&self) -> &LeaderState {
        &self.leader_state
    }

    /// Returns `LogSync`, a struct to help other servers synchronize their log to correspond to the
    /// current state of our own log. The `common_prefix_idx` marks where in the log the other server
    /// needs to be sync from.
    #[allow(dead_code)]
    async fn create_log_sync(
        &self,
        common_prefix_idx: usize,
        other_logs_decided_idx: usize,
    ) -> LogSync<T> {
        let decided_idx = self.internal_storage.lock().await.get_decided_idx();
        let (decided_snapshot, suffix, sync_idx) =
            if T::Snapshot::use_snapshots() && decided_idx > common_prefix_idx {
                // Note: We snapshot from the other log's decided index and not the common prefix because
                // snapshots currently only work on decided entries.
                let (delta_snapshot, compacted_idx) = self
                    .internal_storage
                    .lock()
                    .await
                    .create_diff_snapshot(other_logs_decided_idx)
                    .expect(READ_ERROR_MSG);
                let suffix = self
                    .internal_storage
                    .lock()
                    .await
                    .get_suffix(decided_idx)
                    .expect(READ_ERROR_MSG);
                (delta_snapshot, suffix, compacted_idx)
            } else {
                let suffix = self
                    .internal_storage
                    .lock()
                    .await
                    .get_suffix(common_prefix_idx)
                    .expect(READ_ERROR_MSG);
                (None, suffix, common_prefix_idx)
            };
        LogSync {
            decided_snapshot,
            suffix,
            sync_idx,
            stopsign: self.internal_storage.lock().await.get_stopsign(),
        }
    }
}

#[derive(PartialEq, Debug)]
pub(crate) enum Phase {
    Prepare,
    Accept,
    Recover,
    None,
}

#[derive(PartialEq, Debug)]
pub(crate) enum Role {
    Follower,
    Leader,
}

/// Configuration for `SequencePaxos`.
/// # Fields
/// * `pid`: The unique identifier of this node. Must not be 0.
/// * `peers`: The peers of this node i.e. the `pid`s of the other servers in the configuration.
/// * `flexible_quorum` : Defines read and write quorum sizes. Can be used for different latency vs fault tolerance tradeoffs.
/// * `buffer_size`: The buffer size for outgoing messages.
/// * `batch_size`: The size of the buffer for log batching. The default is 1, which means no batching.
/// * `logger_file_path`: The path where the default logger logs events.
#[derive(Clone, Debug)]
pub(crate) struct SequencePaxosConfig {
    pid: NodeId,
    peers: Vec<NodeId>,
    buffer_size: usize,
    pub(crate) batch_size: usize,
    flexible_quorum: Option<FlexibleQuorum>,
}

impl From<OmniPaxosConfig> for SequencePaxosConfig {
    fn from(config: OmniPaxosConfig) -> Self {
        let pid = config.server_config.pid;
        let peers = config
            .cluster_config
            .nodes
            .into_iter()
            .filter(|x| *x != pid)
            .collect();
        SequencePaxosConfig {
            pid,
            peers,
            flexible_quorum: config.cluster_config.flexible_quorum,
            buffer_size: config.server_config.buffer_size,
            batch_size: config.server_config.batch_size,
        }
    }
}
