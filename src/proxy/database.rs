use tokio::sync::Mutex;
use omnipaxos_kv::common::messages::PRCommand;
use std::{collections::HashMap, sync::Arc};

#[derive(Clone)]
pub struct Database {
    db: Arc<Mutex<HashMap<u64, (usize, Option<Option<Option<String>>>)>>>,
    pub quorum: usize,
}

impl Database {
    pub fn new(q: usize) -> Self {
        Self { 
            db: Arc::new(Mutex::new(HashMap::new())),
            quorum: q
        }
    }

    pub async fn handle_command(&mut self, command: PRCommand) -> Option<Option<(usize, Option<Option<Option<String>>>)>> {
        let mut db = self.db.lock().await;

        match command {
            PRCommand::Put(key, (_, sus)) => {
                if let Some(val) = db.get(&key).cloned() {
                    if  let Some(res) = &val.1 {
                        db.insert(key, (val.0 + 1, Some(res.clone())));
                        None
                    } else {
                        if let Some(res) = sus {
                            db.insert(key, (val.0 + 1, Some(res)));
                            None
                        } else {
                            db.insert(key, (val.0 + 1, val.1.clone()));
                            None
                        }
                    }
                } else {
                    None
                }
            }
            PRCommand::Delete(key) => {
                db.remove(&key);
                None
            }
            PRCommand::Get(key) => { Some(db.get(&key).cloned()) }
        }
    }
}
