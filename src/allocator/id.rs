use crate::{kv::Event, Command, Error, Msg, Res, Result};
use bytes::{BufMut, Bytes, BytesMut};
use crossbeam::channel::Sender;
use futures::{channel::mpsc, StreamExt};
use futures_timer::Delay;
use slog::{debug, error, info, Logger};
use std::{
    convert::TryInto,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use yatp::{task::future::TaskCell, Remote};

static ID_KEY: Bytes = Bytes::from_static(b"did");
const ID_LIMIT_SLEEP: Duration = Duration::from_secs(3);
const ID_LIMIT_STEP: u64 = 10240;
const ID_INIT: u64 = 1;

struct IdWatcher {
    tx: mpsc::Sender<Res>,
    rx: mpsc::Receiver<Res>,
    allocator: IdAllocator,
}

impl IdWatcher {
    async fn init_id_limit(&mut self) -> Option<(u64, Option<u64>)> {
        let msg = Msg::WaitEvent {
            event: Event::CommittedToCurrentTermAsLeader,
            notifier: self.tx.clone(),
        };
        self.allocator.sender.send(msg).unwrap();
        let term = match self.rx.next().await {
            Some(Res::RoleInfo { term, .. }) => term,
            _ => return None,
        };
        info!(self.allocator.logger, "became leader at term {}", term);
        self.allocator
            .sender
            .send(Msg::snapshot(self.tx.clone()))
            .unwrap();
        let snap = match self.rx.next().await {
            Some(Res::Snapshot(s)) => s,
            _ => return None,
        };
        let limit = match snap.get(&*ID_KEY) {
            Ok(Some(val)) => Some(u64::from_le_bytes((&*val).try_into().unwrap())),
            Ok(None) => None,
            Err(e) => panic!("failed to get tso: {}", e),
        };
        Some((term, limit))
    }

    async fn advance_id_limit(&mut self) {
        let (mut term, limit) = match self.init_id_limit().await {
            Some((t, l)) => (t, l),
            None => return,
        };
        let (mut id, mut id_limit) = match limit {
            Some(l) => (l + 1, l + ID_LIMIT_STEP),
            _ => (ID_INIT, ID_INIT + ID_LIMIT_STEP),
        };
        loop {
            let last_term = term;
            let mut value = BytesMut::with_capacity(8);
            value.put_u64_le(id_limit);
            let cmd = Command::put(ID_KEY.clone(), value.freeze());
            self.allocator
                .sender
                .send(Msg::check_term_command(cmd, term, Some(self.tx.clone())))
                .unwrap();
            match self.rx.next().await {
                Some(Res::Success) => {
                    self.allocator.id.val.fetch_max(id, Ordering::SeqCst);
                    self.allocator
                        .id
                        .upper_limit
                        .store(id_limit, Ordering::SeqCst);
                    if self.allocator.id.term.swap(term, Ordering::SeqCst) > term {
                        panic!("invalid term: {} < {}", term, last_term);
                    }
                    debug!(
                        self.allocator.logger,
                        "advance to {} {} {}", id, term, id_limit
                    );
                    loop {
                        Delay::new(ID_LIMIT_SLEEP).await;
                        if self.allocator.id.val.load(Ordering::Relaxed) + ID_LIMIT_STEP / 2
                            >= id_limit
                        {
                            id_limit += ID_LIMIT_STEP;
                            break;
                        }
                    }
                }
                Some(Res::Fail(msg)) => {
                    error!(self.allocator.logger, "failed to write id limit: {}", msg);
                    let last_limit = self.allocator.id.upper_limit.load(Ordering::SeqCst);
                    // Reset tso to avoid extra requests.
                    self.allocator.id.val.store(last_limit, Ordering::SeqCst);
                    match self.init_id_limit().await {
                        Some((t, l)) => match l {
                            Some(l) => {
                                if last_term < t {
                                    id = l + 1;
                                    id_limit = l + ID_LIMIT_STEP;
                                    term = t;
                                } else {
                                    assert_eq!(last_term, t);
                                }
                            }
                            None => {
                                assert_eq!(id, ID_INIT);
                            }
                        },
                        None => {
                            if id != ID_INIT {
                                panic!("tso key must exist");
                            } else {
                                return;
                            }
                        }
                    }
                }
                res => panic!("unexpected result {:?}", res),
            }
        }
    }
}

#[derive(Default)]
struct Id {
    val: AtomicU64,
    term: AtomicU64,
    upper_limit: AtomicU64,
}

#[derive(Clone)]
pub struct IdAllocator {
    sender: Sender<Msg>,
    id: Arc<Id>,
    logger: Logger,
}

impl IdAllocator {
    pub fn new(sender: Sender<Msg>, remote: &Remote<TaskCell>, logger: Logger) -> IdAllocator {
        let allocator = IdAllocator {
            sender,
            id: Arc::new(Id::default()),
            logger,
        };
        let (tx, rx) = mpsc::channel(1);
        let mut watcher = IdWatcher {
            tx,
            rx,
            allocator: allocator.clone(),
        };
        remote.spawn(async move { watcher.advance_id_limit().await });
        allocator
    }

    // A more efficient way is to use lease, which will depend on high accurate
    // time.
    pub async fn alloc(&self, count: u64) -> Result<u64> {
        let term = self.id.term.load(Ordering::SeqCst);
        let mut val = self.id.val.load(Ordering::SeqCst);
        val = loop {
            let limit = self.id.upper_limit.load(Ordering::SeqCst);
            if val >= limit {
                return Err(Error::Other("no tso available".to_string()));
            }
            let new_val = val + count;
            match self.id.val.compare_exchange_weak(
                val,
                new_val,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break new_val,
                Err(v) => val = v,
            }
        };
        let (tx, mut rx) = mpsc::channel(1);
        self.sender
            .send(Msg::check_snapshot(term, tx.clone()))
            .unwrap();
        match rx.next().await {
            Some(Res::Snapshot(_)) => return Ok(val),
            Some(Res::Fail(err)) => return Err(Error::Other(err)),
            res => panic!("unexpected result {:?}", res),
        }
    }
}
