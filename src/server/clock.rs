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
                Clock::resync();
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

    fn resync(&self, real_time: SystemTime) {
        let mut state = self.state.lock().unwrap();
        state.logical_time = real_time;
        state.counter = 0;
    }
}
