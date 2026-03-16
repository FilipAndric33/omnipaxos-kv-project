use omnipaxos_kv::common::messages::PRCommand;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Database {
    db: Arc<Mutex<HashMap<u64, (usize, Option<Option<Option<String>>>)>>>,
    pub quorum: usize,
}

impl Database {
    pub fn new(q: usize) -> Self {
        Self {
            db: Arc::new(Mutex::new(HashMap::new())),
            quorum: q,
        }
    }

    pub async fn handle_command(
        &mut self,
        command: PRCommand,
    ) -> Option<Option<(usize, Option<Option<Option<String>>>)>> {
        let mut db = self.db.lock().await;

        match command {
            PRCommand::Put(key, val) => {
                db.insert(key, val);
                None
            }
            PRCommand::Delete(key) => {
                db.remove(&key);
                None
            }
            PRCommand::Get(key) => Some(db.get(&key).cloned()),
        }
    }
}
