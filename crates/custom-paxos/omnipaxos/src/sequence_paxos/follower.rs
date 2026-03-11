use log::warn;

use super::*;

impl<T, B> SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T> + 'static,
{
    pub(crate) async fn handle_execution<E: Fn(T) -> Option<Option<String>>>(
        &self,
        entry: T,
        accepted_idx: usize,
        e: E,
    ) {
        e(entry);
        self.internal_storage
            .lock()
            .await
            .set_decided_idx(accepted_idx)
            .expect(WRITE_ERROR_MSG);
    }

    pub(crate) fn handle_new_proposal<S: Fn(T) -> Option<Option<String>>>(
        &mut self,
        proxy: NodeId,
        entry: T,
        should_speculate: bool,
        speculate: S,
    ) {
        let speculation = if should_speculate {
            Some(speculate(entry.clone()))
        } else {
            None
        };
        let outgoing = self.outgoing.clone();
        let mypid = self.pid;
        let buffers = self.buffers.clone();
        let storage = self.internal_storage.clone();
        tokio::spawn(async move {
            let notifier = Buffers::insert(buffers, entry).await;
            if let Some(notifier) = notifier {
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
                                msg: PaxosMsg::Ack(req.get_id(), hash, speculation, accepted_idx),
                            }))
                    }
                    Err(e) => {
                        warn!("Failed to wait for the deadline to end: {e:?}");
                    }
                }
            }
        });
    }
}
