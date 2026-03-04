use std::time::{Duration, SystemTime};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use omnipaxos::util::NodeId;
use tokio::time;
use rand::rngs::StdRng;
use rand::{SeedableRng, Rng};   

pub type ClockId = usize;

static CLOCK_COUNTER: AtomicUsize = AtomicUsize::new(1);

pub enum ClockError {
    Ambig(String)
}

#[derive(Clone)]
pub struct Clock {
    state: Arc<Mutex<ClockState>>
}

pub struct ClockState {
    id: ClockId,
    master: NodeId,
    offset: f64,
    drift: Duration,
    uncertainty: Duration,
    sync_freq: u32,
    logical_time: SystemTime,
    counter: u32
}

//The clock is a simulator that can be passed configurable parameters to simulate different states of the system. Current setup provides us with the ticking function which is an async function running every 1ms using tokyo interval, meaning if there is overhead in one loop the wait time in the next one will be lowered which will cumulatively average out to 1ms. There is a counter that is counting each tick, once it reaches the sync freq (which is an int representing the number of millis) it is supposed to resync with the leader's clock. To simulate this ill have to check the functionality of the server.rs and network.rs to check how do we incorporate sending messages to the leader and getting the leader's response to resync ourselves. For the proper clock synchronization we will need to incorporate a couple different terms - clock uncertainty which is a metric defining how many millis the manufacturer (in this case us) can guarantee of error bound. The real clock uncertainty is affected by the clock drift after the resyn, or rather the uncertainty is 0 right after the resync and each iteration we add the worst case scenario of the possible drift. Once the clock reaches the error bound no matter where it is in the alg it needs to resync. Furthermore, we need to calculate the latency from the responses and the one way delay (owd) to increase the precision on the clocks. This needs to be simulated in our system due to the fact that there is almost 0 latency sending messages between processes on the same system. There needs to be added a new abstraction - the stateless proxy - which will delegate the tasks from the client to each of the replicas (clusters) an will incorporate the clock sync logic, meaning we will send our resync req here. Inside the reply, the replica includes its current view-id,replica-id, and the request-id of the corresponding request on the fast path. 

impl Clock {
    pub fn new(mas: NodeId, offset: f64, drift: Duration, uncertainty: Duration, sync_freq: u32) -> Self {
        let id = CLOCK_COUNTER.fetch_add(1, Ordering::SeqCst);
        let master = mas;
        let logical_time = SystemTime::now();
        let mut counter = 0;
        let state = Arc::new(Mutex::new(ClockState {
            id,
            master,
            offset,
            drift,
            uncertainty,
            sync_freq,
            logical_time,
            counter
        }));

        let clock_ref = Arc::clone(&state);
        tokio::spawn(async move {
            Self::tick_fn(clock_ref).await;
        });

        Self { state }
    }

    pub async fn tick_fn(clock_ref: Arc<Mutex<ClockState>>) {
        let mut interval = time::interval(Duration::from_millis(1));
        let mut rng = StdRng::from_entropy();
        loop {
            interval.tick().await;
            let mut c = clock_ref.lock().unwrap();
            c.counter += 1;
            c.logical_time = if rng.gen_bool(0.5) {
                SystemTime::now() + c.drift
            } else {
                SystemTime::now() - c.drift
            };
            if c.counter == c.sync_freq {
                
            }
        }
    }

    pub fn get_time(&self) -> SystemTime {
        self.state.lock().unwrap().logical_time
    }

    pub fn get_uncertainty(&self) -> Duration {
        let state = self.state.lock().unwrap();
        state.counter * state.drift
    }

    pub fn resync(&self, real_time: SystemTime) {
        let mut state = self.state.lock().unwrap();
        state.logical_time = real_time;
        state.counter = 0;
    }
}
