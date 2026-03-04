
use log::*;
use omnipaxos_kv::common::{
    kv::{ClientId, NodeId},
    messages::*,
};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::task::JoinHandle;

use crate::network::Network;

pub struct Proxy {
    _actor: JoinHandle<()>,
    sender: UnboundedSender<ProxyCommand>,
}

pub enum ProxyCommand {
    ForwardClientMessage { from: ClientId, msg: ClientMessage },
    HandleClockRequest { from: NodeId },
}

impl Proxy {
    pub fn new(id: NodeId, peers: Vec<NodeId>, network: &mut Network) -> Self {
        let peer_senders: Vec<(NodeId, PeerOutbox)> = peers
            .iter()
            .map(|&peer_id| (peer_id, network.peer_outbox(peer_id)))
            .collect();

        let (tx, mut rx) = mpsc::unbounded_channel::<ProxyCommand>();

        let actor = tokio::spawn(async move {
            info!("Proxy actor started on leader {id}");
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    ProxyCommand::ForwardClientMessage { from, msg } => {
                        for (peer_id, ref outbox) in &peer_senders {
                            let forward = ClusterMessage::ForwardedClientMessage {
                                client_id: from,
                                msg: msg.clone(),
                            };
                            if let Err(err) = outbox.send(forward) {
                                warn!(
                                    "Proxy: couldn't forward client {from}'s message to \
                                     replica {peer_id}: {err}"
                                );
                            }
                        }
                    }

                    ProxyCommand::HandleClockRequest { from } => {
                        if let Some((_, outbox)) = peer_senders
                            .iter()
                            .find(|(id, _)| *id == from) {
                                let msg: ClusterMessage = ClusterMessage::LeaderTime(from);
                                if let Err(err) = outbox.send(msg) {
                                    warn!("Proxy: couldn't send message to node {from} {err}");
                                }
                            }
                        else {
                            warn!("Proxy: couldn't find the node {from} for the clock resync request.");
                        }
                    }
                }
            }
            info!("Proxy actor on leader {id} shut down");
        });

        Proxy { 
            _actor: actor, 
            sender: tx 
        }
    }

    #[inline]
    pub fn forward_client_message(&self, from: ClientId, msg: ClientMessage) {
        let _ = self
            .sender
            .send(ProxyCommand::ForwardClientMessage { from, msg });
    }

    #[inline]
    pub fn handle_clock_request(&self, from: NodeId) {
        let _ = self.sender.send(ProxyCommand::HandleClockRequest { from });
    }

    pub fn close(self) {
        self._actor.abort();
    }
}

pub type PeerOutbox = UnboundedSender<ClusterMessage>;