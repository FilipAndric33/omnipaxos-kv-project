use log::warn;
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
    ops::Deref,
    sync::Arc,
    time::SystemTime,
};
use tokio::sync::{Mutex, oneshot};

use crate::clock::Clock;

pub trait Request: Send {
    fn get_deadline(&self) -> SystemTime;
    fn get_id(&self) -> usize;
}

struct StoredRequest<R: Request>(R);

impl<R: Request> Deref for StoredRequest<R> {
    type Target = R;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<R: Request> Ord for StoredRequest<R> {
    fn cmp(&self, other: &Self) -> Ordering {
        other.get_deadline().cmp(&self.get_deadline()) // reverse for min-heap
    }
}

impl<R: Request> PartialOrd for StoredRequest<R> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<R: Request> PartialEq for StoredRequest<R> {
    fn eq(&self, other: &Self) -> bool {
        self.get_deadline() == other.get_deadline()
    }
}

impl<R: Request> Eq for StoredRequest<R> {}

#[allow(dead_code)]
pub struct Buffers<R: Request> {
    clock: Clock,
    early: BinaryHeap<StoredRequest<R>>,
    notifiers: HashMap<usize, oneshot::Sender<R>>,
    late: HashMap<usize, R>,
}

pub type BuffersRef<R> = Arc<Mutex<Buffers<R>>>;

#[allow(dead_code)]
impl<R: Request + 'static> Buffers<R> {
    pub fn new(clock: Clock) -> BuffersRef<R> {
        let buffers = Arc::new(Mutex::new(Buffers {
            clock,
            early: BinaryHeap::new(),
            late: HashMap::new(),
            notifiers: HashMap::new(),
        }));

        let buffers_cloned = buffers.clone();
        tokio::spawn(async move {
            Self::waiter(buffers_cloned).await;
        });

        buffers
    }

    async fn waiter(buffers: BuffersRef<R>) {
        loop {
            let (sender, req, clock) = loop {
                let mut locked = buffers.lock().await;
                match locked.early.pop() {
                    Some(req) => {
                        break (
                            locked.notifiers.remove(&req.get_id()),
                            req,
                            locked.clock.clone(),
                        );
                    }
                    None => continue,
                }
            };
            let deadline = req.get_deadline();
            let notifier = clock.add_deadline(deadline).await;
            if let Err(e) = notifier.await {
                warn!("Failed to receiv notification for a deadline: {e}");
                continue;
            }
            match sender {
                Some(sender) => {
                    if let Err(_) = sender.send(req.0) {
                        warn!("Failed to send the request");
                    }
                }
                None => {
                    warn!("Notifiers and early buffer are not synchronized.");
                }
            }
        }
    }

    pub async fn insert(buffers: BuffersRef<R>, r: R) -> Option<oneshot::Receiver<R>> {
        let (tx, rx) = oneshot::channel();
        let mut locked = buffers.lock().await;
        let r = StoredRequest(r);
        locked.notifiers.insert(r.get_id(), tx);
        locked.early.push(r);
        Some(rx)
    }
}
