use crate::{configs::{OmniPaxosKVConfig, ClockConfig}, database::Database, network::Network};
use chrono::Utc;
use log::*;
use omnipaxos::{
    messages::Message,
    util::{LogEntry, NodeId},
    OmniPaxos, OmniPaxosConfig,
};
use omnipaxos_kv::common::{kv::*, messages::*, utils::Timestamp};
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::{
    fs::File,
    io::Write,
    time::{Duration, Instant, SystemTime},
};
use super::clock::*;
use super::proxy::*;

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;
const NETWORK_BATCH_SIZE: usize = 100;
const LEADER_WAIT: Duration = Duration::from_secs(1);
const ELECTION_TIMEOUT: Duration = Duration::from_secs(1);

/// Metrics for benchmark: fast-path ratio (responses not requiring leader/clock intervention).
#[derive(Debug, Default, serde::Serialize)]
pub struct ServerMetrics {
    pub total_responses: u64,
    pub fast_path_responses: u64,
}

pub struct OmniPaxosServer {
    pub id: NodeId,
    database: Database,
    network: Network,
    omnipaxos: OmniPaxosInstance,
    current_decided_idx: usize,
    omnipaxos_msg_buffer: Vec<Message<Command>>,
    pub peers: Vec<NodeId>,
    pub clock: Clock,
    config: OmniPaxosKVConfig,
    proxy: Option<Proxy>,
    last_resync_instant: Option<Instant>,
    metrics: ServerMetrics,
}

impl OmniPaxosServer {
    fn clock_from_config(server_id: NodeId, clock: Option<&ClockConfig>) -> Clock {
        let (uncertainty_us, sync_ms) = clock
            .map(|c| (c.clock_uncertainty_us, c.clock_sync_interval_ms))
            .unwrap_or((10, 1));
        let drift = Duration::from_micros(uncertainty_us);
        Clock::new(server_id, 0.0, drift, drift, sync_ms)
    }

    pub async fn new(config: OmniPaxosKVConfig) -> Self {
        // Initialize OmniPaxos instance
        let storage: MemoryStorage<Command> = MemoryStorage::default();
        let omnipaxos_config: OmniPaxosConfig = config.clone().into();
        let omnipaxos_msg_buffer = Vec::with_capacity(omnipaxos_config.server_config.buffer_size);
        let omnipaxos = omnipaxos_config.build(storage).unwrap();
        // Waits for client and server network connections to be established
        let network = Network::new(config.clone(), NETWORK_BATCH_SIZE).await;
        let clock_cfg = config.local.clock.clone().or_else(ClockConfig::from_env);
        let clock = Self::clock_from_config(config.local.server_id, clock_cfg.as_ref());
        OmniPaxosServer {
            id: config.local.server_id,
            database: Database::new(),
            network,
            omnipaxos,
            current_decided_idx: 0,
            omnipaxos_msg_buffer,
            peers: config.get_peers(config.local.server_id),
            clock,
            config,
            proxy: None,
            last_resync_instant: None,
            metrics: ServerMetrics::default(),
        }
    }

    pub async fn run(&mut self) {
        // Save config to output file
        self.save_output().expect("Failed to write to file");
        let mut client_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        // We don't use Omnipaxos leader election at first and instead force a specific initial leader
        self.establish_initial_leader(&mut cluster_msg_buf, &mut client_msg_buf)
            .await;
        // Main event loop with leader election
        let mut election_interval = tokio::time::interval(ELECTION_TIMEOUT);
        let sync_interval_ms = self
            .config
            .local
            .clock
            .as_ref()
            .map(|c| c.clock_sync_interval_ms)
            .unwrap_or(1);
        let mut clock_sync_interval = tokio::time::interval(Duration::from_millis(sync_interval_ms as u64));
        let _ = clock_sync_interval.tick().await; // skip immediate first tick
        loop {
            tokio::select! {
                _ = election_interval.tick() => {
                    self.omnipaxos.tick();
                    self.send_outgoing_msgs();

                    if let Some((curr_leader, is_accept_phase)) = self.omnipaxos.get_current_leader() {
                        if is_accept_phase {
                            if curr_leader == self.id && self.proxy.is_none() {
                                self.proxy = Some(Proxy::new(self.id.clone(), self.peers.clone(), &mut self.network));
                            }
                        }
                    }
                },
                _ = clock_sync_interval.tick(), if self.proxy.is_some() => {
                    // Leader periodically requests time from each peer so clock resync runs and slow path can be observed.
                    if let Some(proxy) = &self.proxy {
                        for &peer in &self.peers {
                            proxy.handle_clock_request(peer);
                        }
                    }
                },
                _ = self.network.cluster_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE) => {
                    self.handle_cluster_messages(&mut cluster_msg_buf).await;
                },
                _ = self.network.client_messages.recv_many(&mut client_msg_buf, NETWORK_BATCH_SIZE) => {
                    self.handle_client_messages(&mut client_msg_buf).await;
                },
            }
        }
    }

    // Ensures cluster is connected and initial leader is promoted before returning.
    // Once the leader is established it chooses a synchronization point which the
    // followers relay to their clients to begin the experiment.
    async fn establish_initial_leader(
        &mut self,
        cluster_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>,
        client_msg_buffer: &mut Vec<(ClientId, ClientMessage)>,
    ) {
        let mut leader_takeover_interval = tokio::time::interval(LEADER_WAIT);
        loop {
            tokio::select! {
                _ = leader_takeover_interval.tick(), if self.config.cluster.initial_leader == self.id => {
                    if let Some((curr_leader, is_accept_phase)) = self.omnipaxos.get_current_leader(){
                        if curr_leader == self.id && is_accept_phase {
                            info!("{}: Leader fully initialized", self.id);
                            let experiment_sync_start = (Utc::now() + Duration::from_secs(2)).timestamp_millis();
                            self.send_cluster_start_signals(experiment_sync_start);
                            self.send_client_start_signals(experiment_sync_start);
                            self.proxy = Some(Proxy::new(self.id.clone(), self.peers.clone(), &mut self.network));
                            break;
                        }
                    }
                    info!("{}: Attempting to take leadership", self.id);
                    self.omnipaxos.try_become_leader();
                    self.send_outgoing_msgs();
                },
                _ = self.network.cluster_messages.recv_many(cluster_msg_buffer, NETWORK_BATCH_SIZE) => {
                    let recv_start = self.handle_cluster_messages(cluster_msg_buffer).await;
                    if recv_start {
                        break;
                    }
                },
                _ = self.network.client_messages.recv_many(client_msg_buffer, NETWORK_BATCH_SIZE) => {
                    self.handle_client_messages(client_msg_buffer).await;
                },
            }
        }
    }

    fn handle_decided_entries(&mut self) {
        // TODO: Can use a read_raw here to avoid allocation
        let new_decided_idx = self.omnipaxos.get_decided_idx();
        if self.current_decided_idx < new_decided_idx {
            let decided_entries = self
                .omnipaxos
                .read_decided_suffix(self.current_decided_idx)
                .unwrap();
            self.current_decided_idx = new_decided_idx;
            debug!("Decided {new_decided_idx}");
            let decided_commands = decided_entries
                .into_iter()
                .filter_map(|e| match e {
                    LogEntry::Decided(cmd) => Some(cmd),
                    _ => unreachable!(),
                })
                .collect();
            self.update_database_and_respond(decided_commands);
        }
    }

    fn update_database_and_respond(&mut self, commands: Vec<Command>) {
        let sync_interval_ms = self
            .config
            .local
            .clock
            .as_ref()
            .map(|c| c.clock_sync_interval_ms)
            .unwrap_or(1);
        let sync_interval = Duration::from_millis(sync_interval_ms as u64);
        for command in commands {
            let read = self.database.handle_command(command.kv_cmd);
            if command.coordinator_id == self.id {
                self.metrics.total_responses += 1;
                let is_fast_path = self.last_resync_instant.map_or(true, |t| t.elapsed() > sync_interval);
                if is_fast_path {
                    self.metrics.fast_path_responses += 1;
                }
                let response = match read {
                    Some(read_result) => ServerMessage::Read(command.id, read_result),
                    None => ServerMessage::Write(command.id),
                };
                self.network.send_to_client(command.client_id, response);
                self.maybe_flush_metrics();
            }
        }
    }

    fn maybe_flush_metrics(&self) {
        if self.metrics.total_responses % 10 == 0 {
            let path = self.config.local.output_filepath.replace(".json", ".metrics.json");
            if let Ok(mut f) = File::create(&path) {
                let _ = serde_json::to_writer_pretty(&mut f, &self.metrics);
                let _ = f.flush();
            }
        }
    }

    fn send_outgoing_msgs(&mut self) {
        self.omnipaxos
            .take_outgoing_messages(&mut self.omnipaxos_msg_buffer);
        for msg in self.omnipaxos_msg_buffer.drain(..) {
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
            self.network.send_to_cluster(to, cluster_msg);
        }
    }

    async fn handle_client_messages(&mut self, messages: &mut Vec<(ClientId, ClientMessage)>) {
        for (from, message) in messages.drain(..) {
            let ClientMessage::Append(cmd_id, kv_cmd) = message;
            if let Some(proxy) = &self.proxy {
                // We are the leader and have the client connection; we are the coordinator.
                let coordinator_id = self.id;
                proxy.forward_client_message(coordinator_id, from, ClientMessage::Append(cmd_id, kv_cmd.clone()));
                self.append_to_log(coordinator_id, from, cmd_id, kv_cmd);
            } else {
                // Replica: we have the client connection; ask leader to replicate with us as coordinator.
                let coordinator_id = self.id;
                let msg = ClientMessage::Append(cmd_id, kv_cmd);
                for &peer in &self.peers {
                    self.network.send_to_cluster(
                        peer,
                        ClusterMessage::ClientMessageForLeader {
                            coordinator_id,
                            client_id: from,
                            msg: msg.clone(),
                        },
                    );
                }
            }
        }
        self.send_outgoing_msgs();
    }

    fn handle_single_client_message(&mut self, coordinator_id: NodeId, from: ClientId, msg: ClientMessage) {
        let ClientMessage::Append(cmd_id, kv_cmd) = msg;
        self.append_to_log(coordinator_id, from, cmd_id, kv_cmd);
    }

    async fn handle_cluster_messages(
        &mut self,
        messages: &mut Vec<(NodeId, ClusterMessage)>,
    ) -> bool {
        let mut received_start_signal = false; 
        for (from, message) in messages.drain(..) {
            trace!("{}: Received {message:?}", self.id);
            match message {
                ClusterMessage::OmniPaxosMessage(m) => {
                    self.omnipaxos.handle_incoming(m);
                    self.handle_decided_entries();
                }
                ClusterMessage::LeaderStartSignal(start_time) => {
                    debug!("Received start message from peer {from}");
                    received_start_signal = true;
                    self.send_client_start_signals(start_time);
                }
                ClusterMessage::LeaderTime(from) => {
                    let cur: SystemTime = self.clock.get_time();
                    let msg = ClusterMessage::ClockResponse {
                        real_time: cur
                    };
                    self.network.send_to_cluster(from, msg);
                }
                ClusterMessage::ClockResponse { real_time } => {
                    self.clock.resync(real_time);
                    self.last_resync_instant = Some(Instant::now());
                }
                ClusterMessage::ForwardedClientMessage {
                    coordinator_id,
                    client_id,
                    msg,
                } => {
                    self.handle_single_client_message(coordinator_id, client_id, msg);
                }
                ClusterMessage::ClientMessageForLeader {
                    coordinator_id,
                    client_id,
                    msg,
                } => {
                    // Only the leader appends and forwards; replicas ignore (they'll get ForwardedClientMessage).
                    if self.proxy.is_some() {
                        let ClientMessage::Append(cmd_id, kv_cmd) = msg.clone();
                        self.append_to_log(coordinator_id, client_id, cmd_id, kv_cmd);
                        if let Some(proxy) = &self.proxy {
                            proxy.forward_client_message(coordinator_id, client_id, msg);
                        }
                    }
                }
            }
        }
        self.send_outgoing_msgs();
        received_start_signal
    }


    fn append_to_log(
        &mut self,
        coordinator_id: NodeId,
        from: ClientId,
        command_id: CommandId,
        kv_command: KVCommand,
    ) {
        let command = Command {
            client_id: from,
            coordinator_id,
            id: command_id,
            kv_cmd: kv_command,
        };
        self.omnipaxos
            .append(command)
            .expect("Append to Omnipaxos log failed");
    }

    fn send_cluster_start_signals(&mut self, start_time: Timestamp) {
        for peer in &self.peers {
            debug!("Sending start message to peer {peer}");
            let msg = ClusterMessage::LeaderStartSignal(start_time);
            self.network.send_to_cluster(*peer, msg);
        }
    }

    fn send_client_start_signals(&mut self, start_time: Timestamp) {
        for client_id in 1..self.config.local.num_clients as ClientId + 1 {
            debug!("Sending start message to client {client_id}");
            let msg = ServerMessage::StartSignal(start_time);
            self.network.send_to_client(client_id, msg);
        }
    }

    fn save_output(&mut self) -> Result<(), std::io::Error> {
        let config_json = serde_json::to_string_pretty(&self.config)?;
        let mut output_file = File::create(&self.config.local.output_filepath)?;
        output_file.write_all(config_json.as_bytes())?;
        output_file.flush()?;
        let metrics_path = self.config.local.output_filepath.replace(".json", ".metrics.json");
        let mut metrics_file = File::create(metrics_path)?;
        metrics_file.write_all(serde_json::to_string_pretty(&self.metrics)?.as_bytes())?;
        metrics_file.flush()?;
        Ok(())
    }
}
