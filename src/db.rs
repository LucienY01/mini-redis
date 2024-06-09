use bytes::Bytes;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, Notify};
use tokio::time::Instant;

pub struct DbDropGuard {
    db: Db,
}

#[derive(Clone)]
pub struct Db {
    shared: Arc<Shared>,
}

pub struct Shared {
    state: Mutex<State>,
    background_task: Notify,
}

pub struct State {
    entries: HashMap<String, Entry>,
    expirations: BTreeSet<(Instant, String)>,
    /// Map from channel name to sender.
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,
    shutdown: bool,
}

struct Entry {
    data: Bytes,
    expires_at: Option<Instant>,
}

impl DbDropGuard {
    pub fn new() -> Self {
        DbDropGuard { db: Db::new() }
    }

    pub fn db(&self) -> Db {
        self.db.clone()
    }
}

impl Drop for DbDropGuard {
    fn drop(&mut self) {
        self.db.shutdown_clean_task();
    }
}

impl Db {
    pub fn new() -> Db {
        let shared = Arc::new(Shared::new());

        tokio::spawn(clean_expired_tasks(shared.clone()));

        Db { shared }
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    pub fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let expires_at = expire.map(|duration| Instant::now() + duration);

        let mut state = self.shared.state.lock().unwrap();

        let entry = Entry {
            data: value,
            expires_at,
        };

        let old = state.entries.insert(key.clone(), entry);

        if let Some(old) = old {
            if let Some(expires_at) = old.expires_at {
                state.expirations.remove(&(expires_at, key.clone()));
            }
        }

        if let Some(expires_at) = expires_at {
            if let Some(&(earliest, _)) = state.expirations.first() {
                if expires_at < earliest {
                    self.shared.background_task.notify_waiters();
                }
            } else {
                self.shared.background_task.notify_waiters();
            }

            state.expirations.insert((expires_at, key));
        }
    }

    pub fn shutdown_clean_task(&self) {
        let mut state = self.shared.state.lock().unwrap();
        state.shutdown = true;

        self.shared.background_task.notify_waiters();
    }

    pub fn subscribe(&self, channel: &str) -> broadcast::Receiver<Bytes> {
        let mut state = self.shared.state.lock().unwrap();

        match state.pub_sub.get(channel) {
            Some(tx) => tx.subscribe(),
            None => {
                let (tx, rx) = broadcast::channel(1024);
                state.pub_sub.insert(channel.to_string(), tx);
                rx
            }
        }
    }

    /// Publish a message to the channel. Returns the number of subscribers
    /// listening on the channel.
    pub fn publish(&self, channel: String, message: Bytes) -> usize {
        let state = self.shared.state.lock().unwrap();

        let tx = match state.pub_sub.get(&channel) {
            Some(tx) => tx,
            None => return 0,
        };

        tx.send(message).unwrap_or(0)
    }
}

impl Shared {
    pub fn new() -> Shared {
        Shared {
            state: Mutex::new(State::new()),
            background_task: Notify::new(),
        }
    }

    pub fn is_shutdown(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.shutdown
    }

    pub fn clean_expired_tasks(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();
        // to make the compiler happy
        let state = &mut *state;

        if state.shutdown {
            return None;
        }

        let now = Instant::now();

        while let Some(&(expiration, ref key)) = state.expirations.iter().next() {
            if expiration > now {
                return Some(expiration);
            }

            state.entries.remove(key);
            state.expirations.remove(&(expiration, key.clone()));
        }

        None
    }
}

impl State {
    pub fn new() -> State {
        State {
            entries: HashMap::new(),
            expirations: BTreeSet::new(),
            pub_sub: HashMap::new(),
            shutdown: false,
        }
    }
}

async fn clean_expired_tasks(shared: Arc<Shared>) {
    while !shared.is_shutdown() {
        let next_expiration = shared.clean_expired_tasks();

        match next_expiration {
            Some(when) => {
                tokio::select! {
                    _ = tokio::time::sleep_until(when) => {},
                    _ = shared.background_task.notified() => {},
                }
            }
            None => {
                shared.background_task.notified().await;
            }
        }
    }
}
