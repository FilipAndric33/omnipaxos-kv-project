use crate::{configs::OmniPaxosKVConfig, database::Database, network::Network};
use chrono::Utc;
use log::*;
use omnipaxos::clock::*;
use omnipaxos::{
    OmniPaxos, OmniPaxosConfig,
    messages::Message,
    util::{LogEntry, NodeId},
};
use omnipaxos_kv::common::{kv::*, messages::*, utils::Timestamp};
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::{
    fs::File,
    io::Write,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;
const NETWORK_BATCH_SIZE: usize = 100;
const LEADER_WAIT: Duration = Duration::from_secs(1);
const ELECTION_TIMEOUT: Duration = Duration::from_secs(1);

pub struct OmniPaxosServer {
    pub id: NodeId,
    database: Arc<Mutex<Database>>,
    network: Network,
    omnipaxos: OmniPaxosInstance,
    current_decided_idx: usize,
    omnipaxos_msg_buffer: Vec<Message<Command>>,
    pub peers: Vec<NodeId>,
    pub clock: Clock,
    config: OmniPaxosKVConfig,
}

impl OmniPaxosServer {
    pub async fn new(config: OmniPaxosKVConfig) -> Self {
        // Initialize OmniPaxos instance
        let storage: MemoryStorage<Command> = MemoryStorage::default();
        let omnipaxos_config: OmniPaxosConfig = config.clone().into();
        let omnipaxos_msg_buffer = Vec::with_capacity(omnipaxos_config.server_config.buffer_size);
        let clock = Clock::new(
            config.local.server_id,
            0.0,
            Duration::new(0, 10),
            Duration::new(10, 0),
            1000,
        );
        let omnipaxos = omnipaxos_config
            .build(storage, clock.clone())
            .await
            .unwrap();
        // Waits for client and server network connections to be established
        let network = Network::new(config.clone(), NETWORK_BATCH_SIZE).await;
        OmniPaxosServer {
            id: config.local.server_id,
            database: Arc::new(Mutex::new(Database::new())),
            network,
            omnipaxos,
            current_decided_idx: 0,
            omnipaxos_msg_buffer,
            peers: config.get_peers(config.local.server_id),
            clock,
            config,
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
        loop {
            tokio::select! {
                _ = election_interval.tick() => {
                    self.omnipaxos.tick().await;
                    self.send_outgoing_msgs().await;

                    /*
                    if let Some((curr_leader, is_accept_phase)) = self.omnipaxos.get_current_leader() {
                        if is_accept_phase {
                            if curr_leader == self.id && self.proxy.is_none() {
                                self.proxy = Some(Proxy::new(self.id.clone(), self.peers.clone(), &mut self.network));
                            }
                        }
                    }*/
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
                    if let Some((curr_leader, is_accept_phase)) = self.omnipaxos.get_current_leader().await{
                        if curr_leader == self.id && is_accept_phase {
                            info!("{}: Leader fully initialized", self.id);
                            let experiment_sync_start = (Utc::now() + Duration::from_secs(2)).timestamp_millis();
                            self.send_cluster_start_signals(experiment_sync_start);
                            self.send_client_start_signals(experiment_sync_start);
                            break;
                        }
                    }
                    info!("{}: Attempting to take leadership", self.id);
                    self.omnipaxos.try_become_leader().await;
                    self.send_outgoing_msgs().await;
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

    async fn handle_decided_entries(&mut self) {
        let new_decided_idx = self
            .omnipaxos
            .seq_paxos
            .internal_storage
            .lock()
            .await
            .get_decided_idx();
        if self.current_decided_idx < new_decided_idx {
            let decided_entries = self
                .omnipaxos
                .read_decided_suffix(self.current_decided_idx)
                .await
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
            self.update_database_and_respond(decided_commands).await;
        }
    }

    async fn update_database_and_respond(&mut self, commands: Vec<Command>) {
        for command in commands {
            let read = self.database.lock().unwrap().handle_command(command.kv_cmd);
            if command.coordinator_id == self.id {
                let response = match read {
                    Some(read_result) => ServerMessage::Read(command.id, read_result),
                    None => ServerMessage::Write(command.id),
                };
                self.network.send_to_client(command.client_id, response);
            }
        }
    }

    async fn send_outgoing_msgs(&mut self) {
        self.omnipaxos
            .take_outgoing_messages(&mut self.omnipaxos_msg_buffer)
            .await;
        for msg in self.omnipaxos_msg_buffer.drain(..) {
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
            self.network.send_to_cluster(to, cluster_msg);
        }
    }

    async fn handle_client_messages(&mut self, messages: &mut Vec<(ClientId, ClientMessage)>) {
        for (from, message) in messages.drain(..) {
            match message {
                ClientMessage::Append(_, _) => {
                    self.handle_single_client_message(from, message);
                }
            }
        }
        self.send_outgoing_msgs().await;
    }

    fn handle_single_client_message(&self, _from: ClientId, _msg: ClientMessage) {
        debug!("here")
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
                    let database = self.database.clone();
                    let execute =
                        move |entry: Command| database.lock().unwrap().handle_command(entry.kv_cmd);
                    self.omnipaxos.handle_incoming(m, execute).await;
                    self.handle_decided_entries().await;
                }
                ClusterMessage::LeaderStartSignal(start_time) => {
                    debug!("Received start message from peer {from}");
                    received_start_signal = true;
                    self.send_client_start_signals(start_time);
                }
                ClusterMessage::LeaderTime(from) => {
                    let cur: SystemTime = self.clock.get_time();
                    let msg = ClusterMessage::ClockResponse { real_time: cur };
                    self.network.send_to_cluster(from, msg);
                }
                ClusterMessage::ClockResponse { real_time } => {
                    self.clock.resync(real_time);
                }
                ClusterMessage::ForwardedClientMessage { client_id, msg } => {
                    self.handle_single_client_message(client_id, msg)
                }
            }
        }
        self.send_outgoing_msgs().await;
        received_start_signal
    }

    #[allow(dead_code)]
    async fn append_to_log(
        &mut self,
        from: ClientId,
        command_id: CommandId,
        kv_command: KVCommand,
    ) {
        let command = Command {
            deadline: self.clock.new_deadline(),
            client_id: from,
            coordinator_id: self.id,
            id: command_id,
            kv_cmd: kv_command,
        };
        let database = self.database.clone();
        let speculate = move |entry: Command| database.lock().unwrap().handle_command(entry.kv_cmd);
        self.omnipaxos
            .append(from, command, speculate)
            .await
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
        Ok(())
    }
}
