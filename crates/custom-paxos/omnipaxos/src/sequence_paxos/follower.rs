use std::time::SystemTime;

use log::warn;

use super::*;

impl<T, B> SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T> + 'static,
{
    pub(crate) fn handle_execution<E: Fn(T) -> Option<Option<String>>>(
        &self,
        entry: T,
        accepted_idx: usize,
        e: E,
    ) {
        if matches!(self.state, (Role::Follower, _)) {
            e(entry);
        }
        let storage = self.internal_storage.clone();
        tokio::spawn(async move {
            storage
                .lock()
                .await
                .set_decided_idx(accepted_idx)
                .expect(WRITE_ERROR_MSG);
        });
    }

    pub(crate) fn handle_new_deadline(&self, id: usize, new_deadline: SystemTime) {
        let buffers = self.buffers.clone();
        tokio::spawn(async move { Buffers::free_from_late(buffers, id, new_deadline).await });
    }

    pub(crate) fn handle_new_proposal<S: Fn(T) -> Option<Option<String>>>(
        &mut self,
        proxy: NodeId,
        entry: T,
        im_leader: bool,
        speculate: S,
    ) {
        let speculation = if im_leader {
            Some(speculate(entry.clone()))
        } else {
            None
        };
        let outgoing = self.outgoing.clone();
        let mypid = self.pid;
        let entry_id = entry.get_id();
        let peers = self.peers.clone();
        let buffers = self.buffers.clone();
        let storage = self.internal_storage.clone();
        tokio::spawn(async move {
            let (notifier, new_deadline) = Buffers::insert(buffers, entry, im_leader).await;
            if let Some(new_deadline) = new_deadline {
                let mut locked = outgoing.lock().await;
                for peer in peers {
                    locked.push(Message::SequencePaxos(PaxosMessage {
                        from: mypid,
                        to: peer,
                        msg: PaxosMsg::NewDeadline(entry_id, new_deadline),
                    }));
                }
            }
            match notifier.await {
                Ok(req) => {
                    let (accepted_idx, hash) = {
                        let mut locked = storage.lock().await;

                        (
                            locked
                                .append_entries_and_get_accepted_idx(vec![req.clone()])
                                .expect(WRITE_ERROR_MSG)
                                .unwrap(),
                            locked.storage.history_hash(),
                        )
                    };
                    outgoing
                        .lock()
                        .await
                        .push(Message::SequencePaxos(PaxosMessage {
                            from: mypid,
                            to: proxy,
                            msg: PaxosMsg::Ack(req, hash, speculation, accepted_idx),
                        }))
                }
                Err(e) => {
                    warn!("Failed to wait for the deadline to end: {e:?}");
                }
            }
        });
    }
}
