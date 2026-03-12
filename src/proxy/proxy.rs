use std::{collections::HashMap, sync::Arc};
use omnipaxos_kv::common::{
    kv::NodeId,
    messages::*, utils::{frame_clients_connection, frame_registration_connection, frame_servers_connection},
};
use log::*;
use tokio::{sync::mpsc::{Receiver, Sender}};
use tokio::net::{TcpListener, TcpStream};
use futures::{SinkExt, StreamExt};
use super::database::*;

use crate::configs::ProxyConfig;

const NETWORK_BATCH_SIZE: usize = 100;
pub struct Proxy {
    config: ProxyConfig,
    db: Database
}

impl Proxy {
    pub async fn new(config: ProxyConfig) -> Self {
        let quroum = config.nodes.len() - config.fault_tolerance;
        Proxy {
            config,
            db: Database::new(quroum)
        }
    }

    pub async fn run(&mut self) {
        let mut server_receivers: HashMap<usize, Receiver<ServerMessage>> = HashMap::new();
        let mut client_senders: HashMap<usize, Sender<ClientMessage>> = HashMap::new();
        
        for (i, node) in self.config.nodes.iter().enumerate() {
            let addr = format!("{}:{}", node.address, node.listening_port);
            let (sr_tx,sr_rx) = tokio::sync::mpsc::channel::<ServerMessage>(NETWORK_BATCH_SIZE);
            let (cl_tx, cl_rx) = tokio::sync::mpsc::channel::<ClientMessage>(NETWORK_BATCH_SIZE);
            server_receivers.insert(i, sr_rx);
            client_senders.insert(i, cl_tx);
            tokio::spawn(Self::server_actor(addr, node.id, cl_rx, sr_tx));
        }

        let listen_addr = format!("{}:{}", self.config.listen_address, self.config.listen_port);
        let listener = TcpListener::bind(&listen_addr).await.unwrap_or_else(|e| panic!("Could not bind a proxy listener on {listen_addr}, error: {e}"));
        info!("Proxy listening on {listen_addr}");

        let mut counter: usize = 0;
        loop {
            tokio::select! {
                Ok((client_stream, sock_addr)) = listener.accept() => {
                    info!("Client connected from {sock_addr}");
                    counter += 1;
                    client_stream.set_nodelay(true).unwrap();
                    let client_db = self.db.clone();
                    tokio::spawn(Proxy::handle_client_requests(client_db, client_stream, server_receivers.remove(&counter).unwrap(), client_senders.clone(), counter));
                }
            }
        }
    }

        async fn handle_client_requests(mut db: Database, mut client_stream: TcpStream,mut client_receiver: Receiver<ServerMessage>, serv_set: HashMap<usize, Sender<ClientMessage>>, connection_id: usize) {
            let mut reg = frame_registration_connection(client_stream);
            match reg.next().await {
                Some(Ok(RegistrationMessage::ClientRegister)) => {}
                Some(Err(er)) => {
                    error!("Error while connecting proxy to the client: {}", er)
                }
                msg => {
                    error!("Unexpected error occured during proxy connecting to the client.")
                }
            }

            let underlying = reg.into_inner().into_inner();
            let (mut reader, mut writer) = frame_servers_connection(underlying);
            let serv_set = Arc::new(serv_set);

            loop {
                tokio::select! {
                    msg = reader.next() => {
                        match msg {
                            Some(Ok(client_message)) => {
                                let client_message: ClientMessage = client_message;
                                let server_sender = serv_set.get(&connection_id).expect("error getting the server sender");
                                if let Err(e) = server_sender.send(client_message).await {
                                    error!("Error sending message to server. ({e})");
                                    return;
                                }
                            }
                            Some(Err(e)) => {
                                error!("There was an error sent by the client: {e}");
                                return;
                            }
                            None => {
                                info!("Client disconnected.");
                                return;
                            }
                        }
                    }
                    Some(serv_res) = client_receiver.recv() => {
                        if let ServerMessage::Ack(cmd, hash, suspect, last_idx) = serv_res.clone() {
                            db.handle_command(PRCommand::Put(hash, (0, suspect))).await;
                            if let Some(val) = db.handle_command(PRCommand::Get(hash)).await {
                                let val = val.unwrap().clone();
                                if (val.0 >= db.quorum) && (val.1 != None) {
                                    for (_, sender) in serv_set.iter() {
                                        if let Err(e) = sender.send(ClientMessage::Ack(cmd.clone(), last_idx)).await {
                                            error!("Error while fanning out messages to the servers. ({e})");
                                            return;
                                        }
                                    }
                                    if let Err(e) = writer.send(serv_res).await {
                                        error!("Failed to send the server response to client ({e})");
                                        return;
                                    }
                                }
                            }
                        } 
                    }
                }
            }
        }

    async fn server_actor(addr: String, node_id: NodeId,mut server_receiver: Receiver<ClientMessage>, client_sender: Sender<ServerMessage>) {
        loop {
            match tokio::net::TcpStream::connect(&addr).await {
                Err(e) => {
                    error!("Failed to connect to server {node_id}, trying to reconnect in 1 sec.. ({e})");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                },
                Ok(stream) => {
                    stream.set_nodelay(true).unwrap();
                    let mut reg = frame_registration_connection(stream);
                    if let Err(er) = reg.send(RegistrationMessage::ClientRegister).await {
                        error!("Handshake with server unsucssessfull");
                        continue;
                    }
                    let underlying = reg.into_inner().into_inner();
                    let (mut reader, mut writer) = frame_clients_connection(underlying);

                    loop {
                        tokio::select! {
                            Some(msg) = server_receiver.recv() => {
                                if let Err(err) = writer.send(msg).await {
                                    error!("Send to server {node_id} failed, reconnecting.. ({err})");
                                    break;
                                }
                            }
                            Some(msg) = reader.next() => {
                                match msg {
                                    Ok(m) => { let _ = client_sender.send(m).await; }
                                    Err(err) => {
                                        error!("Receive from server {node_id} failed, reconnecting.. ({err})");
                                        break;
                                    }
                                }
                            }
                        }
                }
                }
            }
        }
    }
}