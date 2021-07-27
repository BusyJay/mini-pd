use crate::{kv::Event, Command, Error, Msg, Res, Result};
use bytes::{BufMut, Bytes, BytesMut};
use crossbeam::channel::Sender;
use futures::{channel::mpsc, StreamExt};
use futures_timer::Delay;
use kvproto::pdpb::Timestamp;
use slog::{debug, error, info, Logger};
use std::{
    convert::TryInto,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use yatp::{task::future::TaskCell, Remote};

static TSO_KEY: Bytes = Bytes::from_static(b"dtso");
const TSO_LIMIT_STEP: Duration = Duration::from_secs(4);
const TSO_LIMIT_SLEEP: Duration = Duration::from_secs(3);
const PHYSICAL_OFFSET: u64 = 18;
const LOGICAL_MASK: u64 = (1 << PHYSICAL_OFFSET) - 1;

fn make_tso() -> u64 {
    let dur = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    (dur.as_millis() as u64) << PHYSICAL_OFFSET
}

fn delay_tso(tso: u64, time: Duration) -> u64 {
    tso + ((time.as_millis() as u64) << PHYSICAL_OFFSET)
}

pub fn fill_timestamp(tso: u64, ts: &mut Timestamp) {
    ts.set_physical((tso >> PHYSICAL_OFFSET) as i64);
    ts.set_logical((tso & LOGICAL_MASK) as i64);
}

struct TsoWatcher {
    tx: mpsc::Sender<Res>,
    rx: mpsc::Receiver<Res>,
    allocator: TsoAllocator,
}

impl TsoWatcher {
    async fn init_tso_limit(&mut self) -> Option<(u64, Option<u64>)> {
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
        let limit = match snap.get(&*TSO_KEY) {
            Ok(Some(val)) => Some(u64::from_le_bytes((&*val).try_into().unwrap())),
            Ok(None) => None,
            Err(e) => panic!("failed to get tso: {}", e),
        };
        Some((term, limit))
    }

    async fn advance_tso_limit(&mut self) {
        let (mut term, mut limit) = match self.init_tso_limit().await {
            Some((t, l)) => (t, l),
            None => return,
        };
        loop {
            let tso = make_tso();
            let tso_limit = delay_tso(tso, TSO_LIMIT_STEP);
            let (tso, tso_limit) = match limit {
                Some(l) if l >= tso_limit => (l + 1, delay_tso(l, Duration::from_secs(2))),
                _ => (tso, tso_limit),
            };
            let mut value = BytesMut::with_capacity(8);
            value.put_u64_le(tso_limit);
            let cmd = Command::put(TSO_KEY.clone(), value.freeze());
            self.allocator
                .sender
                .send(Msg::check_term_command(cmd, term, Some(self.tx.clone())))
                .unwrap();
            match self.rx.next().await {
                Some(Res::Success) => {
                    self.allocator.tso.val.fetch_max(tso, Ordering::SeqCst);
                    self.allocator
                        .tso
                        .upper_limit
                        .store(tso_limit, Ordering::SeqCst);
                    let last_term = self.allocator.tso.term.swap(term, Ordering::SeqCst);
                    if last_term > term {
                        panic!("invalid term: {} < {}", term, last_term);
                    }
                    limit = Some(tso_limit);
                    debug!(
                        self.allocator.logger,
                        "advance to {} {} {}", tso, term, tso_limit
                    );
                    Delay::new(TSO_LIMIT_SLEEP).await;
                }
                Some(Res::Fail(msg)) => {
                    error!(self.allocator.logger, "failed to write tso limit: {}", msg);
                    let last_limit = self.allocator.tso.upper_limit.load(Ordering::SeqCst);
                    // Reset tso to avoid extra requests.
                    self.allocator.tso.val.store(last_limit, Ordering::SeqCst);
                    match self.init_tso_limit().await {
                        Some((t, l)) => {
                            term = t;
                            limit = l;
                        }
                        None => {
                            if limit.is_some() {
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
struct Tso {
    val: AtomicU64,
    term: AtomicU64,
    upper_limit: AtomicU64,
}

#[derive(Clone)]
pub struct TsoAllocator {
    sender: Sender<Msg>,
    tso: Arc<Tso>,
    logger: Logger,
}

impl TsoAllocator {
    pub fn new(sender: Sender<Msg>, remote: &Remote<TaskCell>, logger: Logger) -> TsoAllocator {
        let allocator = TsoAllocator {
            sender,
            tso: Arc::new(Tso::default()),
            logger,
        };
        let (tx, rx) = mpsc::channel(1);
        let mut watcher = TsoWatcher {
            tx,
            rx,
            allocator: allocator.clone(),
        };
        remote.spawn(async move { watcher.advance_tso_limit().await });
        allocator
    }

    // A more efficient way is to use lease, which will depend on high accurate
    // time.
    pub async fn alloc(&self, count: u64) -> Result<u64> {
        let term = self.tso.term.load(Ordering::SeqCst);
        let mut val = self.tso.val.load(Ordering::SeqCst);
        val = loop {
            let limit = self.tso.upper_limit.load(Ordering::SeqCst);
            if val >= limit {
                return Err(Error::Other("no tso available".to_string()));
            }
            let new_val = val + count;
            match self.tso.val.compare_exchange_weak(
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
