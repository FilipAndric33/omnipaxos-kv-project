use crate::messages::Message;
use crate::storage::Entry;
use crate::util::NodeId;
use log::{info, warn};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};
use tokio::sync::oneshot;
use tokio::sync::{
    Mutex,
    mpsc::{Receiver, Sender, channel},
};
use tokio::time;

pub type ClockId = usize;

static CLOCK_COUNTER: AtomicUsize = AtomicUsize::new(1);

pub enum ClockError {
    Ambig(String),
}

#[derive(Clone)]
pub struct Clock<T: Entry> {
    pub state: Arc<Mutex<ClockState<T>>>,
}

#[allow(dead_code)]
pub struct ClockState<T: Entry> {
    id: ClockId,
    master: NodeId,
    offset: f64,
    drift: Duration,
    uncertainty: Duration,
    sync_freq: u32,
    logical_time: SystemTime,
    counter: u32,
    deadlines: BinaryHeap<Reverse<SystemTime>>,
    database: HashMap<SystemTime, oneshot::Sender<()>>,
    master_time_receiver: Arc<Mutex<Receiver<SystemTime>>>,
    pub master_time_sender: Sender<SystemTime>,
    outgoing: Arc<Mutex<Vec<Message<T>>>>,
}

//The clock is a simulator that can be passed configurable parameters to simulate different states of the system. Current setup provides us with the ticking function which is an async function running every 1ms using tokyo interval, meaning if there is overhead in one loop the wait time in the next one will be lowered which will cumulatively average out to 1ms. There is a counter that is counting each tick, once it reaches the sync freq (which is an int representing the number of millis) it is supposed to resync with the leader's clock. To simulate this ill have to check the functionality of the server.rs and network.rs to check how do we incorporate sending messages to the leader and getting the leader's response to resync ourselves. For the proper clock synchronization we will need to incorporate a couple different terms - clock uncertainty which is a metric defining how many millis the manufacturer (in this case us) can guarantee of error bound. The real clock uncertainty is affected by the clock drift after the resyn, or rather the uncertainty is 0 right after the resync and each iteration we add the worst case scenario of the possible drift. Once the clock reaches the error bound no matter where it is in the alg it needs to resync. Furthermore, we need to calculate the latency from the responses and the one way delay (owd) to increase the precision on the clocks. This needs to be simulated in our system due to the fact that there is almost 0 latency sending messages between processes on the same system. There needs to be added a new abstraction - the stateless proxy - which will delegate the tasks from the client to each of the replicas (clusters) an will incorporate the clock sync logic, meaning we will send our resync req here. Inside the reply, the replica includes its current view-id,replica-id, and the request-id of the corresponding request on the fast path.

impl<T: Entry> Clock<T> {
    pub fn new(
        mas: NodeId,
        offset: f64,
        drift: Duration,
        uncertainty: Duration,
        sync_freq: u32,
        outgoing: Arc<Mutex<Vec<Message<T>>>>,
    ) -> Self {
        let id = CLOCK_COUNTER.fetch_add(1, Ordering::SeqCst);
        let master = mas;
        let logical_time = SystemTime::now();
        let counter = 0;
        let (master_time_sender, master_time_receiver) = channel::<SystemTime>(100);
        let state = Arc::new(Mutex::new(ClockState {
            id,
            master,
            offset,
            drift,
            uncertainty,
            sync_freq,
            logical_time,
            counter,
            deadlines: BinaryHeap::new(),
            database: HashMap::new(),
            master_time_sender,
            master_time_receiver: Arc::new(Mutex::new(master_time_receiver)),
            outgoing,
        }));

        let clock_ref = Arc::clone(&state);
        tokio::spawn(async move {
            Self::tick_fn(clock_ref).await;
        });

        Self { state }
    }

    pub async fn set_outgoing(&self, outgoing: Arc<Mutex<Vec<Message<T>>>>) {
        self.state.lock().await.outgoing = outgoing;
    }

    pub async fn tick_fn(clock_ref: Arc<Mutex<ClockState<T>>>) {
        let mut interval = time::interval(Duration::from_millis(1));
        let mut rng = StdRng::from_entropy();
        loop {
            interval.tick().await;
            let mut c = clock_ref.lock().await;
            c.counter += 1;

            let step = Duration::from_millis(1);
            let drift = if rng.gen_bool(0.5) {
                step + c.drift
            } else {
                step.saturating_sub(c.drift)
            };
            c.logical_time += drift;

            Self::process_deadlines(&mut c);

            let mut do_sync = false;
            if c.counter >= c.sync_freq {
                c.counter = 0;
                do_sync = true;
            }
            drop(c);
            if do_sync {
               // info!("Processing resync...");
                let clock_ref = clock_ref.clone();
                tokio::spawn(async move {
                    (Self { state: clock_ref }).sync_with_master().await;
                    //info!("Resynced successfully.")
                });
            }
        }
    }

    fn process_deadlines(c: &mut ClockState<T>) {
        while c
            .deadlines
            .peek()
            .map(|deadline| deadline.0 < c.logical_time)
            .unwrap_or(false)
        {
            let deadline = c.deadlines.pop().unwrap().0;
            match c.database.remove(&deadline) {
                Some(sender) => {
                    tokio::spawn(async move {
                        sender.send(()).unwrap();
                    });
                }
                None => {
                    warn!("database and deadlines are not sync.");
                }
            }
        }
    }

    pub async fn get_time(&self) -> SystemTime {
        self.state.lock().await.logical_time
    }

    pub async fn new_deadline(&self) -> SystemTime {
        let logical_time = self.state.lock().await.logical_time;
        logical_time + Duration::from_millis(20)
    }

    pub async fn get_uncertainty(&self) -> Duration {
        let state = self.state.lock().await;
        state.uncertainty + state.counter * state.drift
    }

    pub async fn resync(&self, real_time: SystemTime) {
        let mut state = self.state.lock().await;
        state.logical_time = real_time;
        state.counter = 0;
        state.uncertainty = Duration::ZERO;
    }

    async fn sync_with_master(&self) {
        let (me, outgoing, receiver) = {
            let state = self.state.lock().await;
            (
                state.master,
                state.outgoing.clone(),
                state.master_time_receiver.clone(),
            )
        };

        let t0 = SystemTime::now();

        let leader_time = {
            if me == 1 {
                self.get_time().await
            } else {
                outgoing.lock().await.push(Message::SequencePaxos(
                    crate::messages::sequence_paxos::PaxosMessage {
                        from: me,
                        to: 1,
                        msg: crate::messages::sequence_paxos::PaxosMsg::SyncReq,
                    },
                ));
                let mut receiver = receiver.lock().await;
                receiver.recv().await.unwrap()
            }
        };

        let t1 = SystemTime::now();

        let rtt = t1.duration_since(t0).unwrap();
        let owd = rtt / 2;

        let corrected = leader_time + owd;

        self.resync(corrected).await;
    }

    pub async fn add_deadline(self, deadline: SystemTime) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        let mut locked = self.state.lock().await;
        locked.deadlines.push(Reverse(deadline));
        locked.database.insert(deadline, tx);
        rx
    }
}
