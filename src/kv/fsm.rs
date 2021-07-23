use super::storage::{self, address_key, valid_data_key};
use super::{Command, InvokeContext, Msg, RaftClient, RockStorage};
use crate::{r, Config, Result};
use crossbeam::channel::{self, Receiver, RecvTimeoutError, Sender};
use futures_timer::Delay;
use raft::eraftpb::{Entry, Message};
use raft::prelude::*;
use rocksdb::{ReadOptions, SeekKey, Writable, WriteBatch, DB};
use slog::{info, o, Logger};
use std::convert::TryInto;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use yatp::task::future::TaskCell;
use yatp::Remote;

const SYNC_INTERVAL: Duration = Duration::from_micros(100);

pub struct Fsm {
    node: RawNode<RockStorage>,
    receiver: Receiver<Msg>,
    sender: Sender<Msg>,
    pool: Remote<TaskCell>,
    db: Arc<DB>,
    logger: Logger,
    has_ready: bool,
    abort: bool,
    processed_msg_cnt: usize,
    write_batch: WriteBatch,
    unsynced_data_size: u64,
    persisted_messages: Vec<Vec<Message>>,
    last_ready_number: u64,
    last_sync_time: Instant,
    raft_client: RaftClient,
}

impl Fsm {
    pub fn new(
        config: &Config,
        raft_client: RaftClient,
        logger: &Logger,
        pool: Remote<TaskCell>,
    ) -> Result<Fsm> {
        if !storage::exists(&config.data_dir) {
            if config.initial_peers.contains(&config.my_id) {
                super::bootstrap(
                    &config.data_dir,
                    raft_client.address_map(),
                    &config.initial_peers,
                    config.my_id,
                )?;
            } else {
                super::bootstrap(
                    &config.data_dir,
                    raft_client.address_map(),
                    &[],
                    config.my_id,
                )?;
            }
            info!(
                logger,
                "bootstrapped data dir at {}",
                config.data_dir.display()
            );
        }
        let storage = RockStorage::open(&config.data_dir, config.my_id)?;
        let db = storage.db();
        let cfg = raft::Config {
            id: storage.id(),
            applied: storage.applied(),
            ..Default::default()
        };
        let node = RawNode::new(&cfg, storage, logger)?;
        let logger = logger.new(o! {"fsm_id" => node.store().id()});
        let (tx, rx) = channel::bounded(4096);
        let fsm = Fsm {
            node,
            receiver: rx,
            sender: tx,
            logger,
            db,
            raft_client,
            pool,
            has_ready: false,
            abort: false,
            processed_msg_cnt: 0,
            write_batch: WriteBatch::with_capacity(4096),
            unsynced_data_size: 0,
            persisted_messages: Vec::with_capacity(4096),
            last_ready_number: 0,
            last_sync_time: Instant::now(),
        };
        fsm.load_address_map();
        Ok(fsm)
    }

    fn load_address_map(&self) {
        let mut opt = ReadOptions::default();
        opt.set_iterate_upper_bound(address_key(u64::MAX).to_vec());
        opt.fill_cache(false);
        let map = self.raft_client.address_map();
        let mut address = map.lock();
        let db = self.node.store().db();
        let mut iter = db.iter_opt(opt);
        if iter.seek(SeekKey::Key(&address_key(0))).unwrap() {
            let id = u64::from_be_bytes(iter.key()[1..].try_into().unwrap());
            let addr = String::from_utf8(iter.value().to_vec()).unwrap();
            address.insert(id, addr);
        }
    }

    pub fn sender(&self) -> Sender<Msg> {
        self.sender.clone()
    }

    pub fn id(&self) -> u64 {
        self.node.store().id()
    }

    fn schedule_tick(&mut self) {
        let sender = self.sender.clone();
        self.pool.spawn(async move {
            Delay::new(Duration::from_millis(200)).await;
            // TODO: use unbounded channel instead.
            sender.try_send(Msg::Tick).unwrap_or_else(|e| {
                panic!("failed to send tick: {:?}", e);
            })
        });
    }

    fn process(&mut self, msg: Msg) {
        match msg {
            Msg::Command(c) => {
                let (context, data) = c.into_proposal();
                match self.node.propose(context, data) {
                    Ok(()) => self.has_ready = true,
                    Err(e) => info!(self.logger, "failed to make propose {}", e),
                }
            }
            Msg::RaftMessage(msg) => {
                if let Err(e) = self.node.step(msg) {
                    info!(self.logger, "failed to step message {}", e);
                    self.has_ready = true;
                }
            }
            Msg::Tick => {
                self.has_ready |= self.node.tick();
                self.schedule_tick();
            }
            Msg::Stop => self.abort = true,
        }
    }

    fn handle_committed_entries(&mut self, entries: Vec<Entry>) -> u64 {
        let applied_index = entries.last().unwrap().get_index();
        for mut entry in entries {
            let context = entry.take_context();
            let data = entry.take_data();
            match Command::from_proposal(context, data) {
                None => continue,
                Some(Command::Put { key, value }) => {
                    if valid_data_key(&key) {
                        if let Err(e) = self.write_batch.put(&*key, &*value) {
                            panic!("unable to apply command: {:?}", e);
                        }
                    }
                }
                Some(Command::UpdateAddress { id, address }) => {
                    if let Err(e) = self.write_batch.put(&address_key(id), address.as_bytes()) {
                        panic!("unable to write address at {}: {}", entry.get_index(), e);
                    }
                    self.raft_client.address_map().lock().insert(id, address);
                }
            }
        }
        applied_index
    }

    fn send_messages(&mut self, msgs: Vec<Message>) {
        for msg in msgs {
            self.raft_client.send(msg);
        }
    }

    fn process_read(&mut self, _read_states: Vec<ReadState>) {
        // We don't use read index yet.
    }

    fn process_ready(&mut self) -> Result<()> {
        self.has_ready = false;
        let mut sync_log = false;
        let before_ready = Instant::now();
        if self.node.has_ready() {
            let mut ready = self.node.ready();
            let applied_index = if !ready.committed_entries().is_empty() {
                Some(self.handle_committed_entries(ready.take_committed_entries()))
            } else {
                None
            };
            if !ready.messages().is_empty() {
                self.send_messages(ready.take_messages());
            }
            if !ready.read_states().is_empty() {
                self.process_read(ready.take_read_states());
            }
            self.last_ready_number = ready.number();
            let mut context = InvokeContext::new(self.node.store());
            if let Some(index) = applied_index {
                context.apply_state.set_applied_index(index);
            }
            self.node
                .mut_store()
                .process_ready(&mut context, &mut ready, &mut self.write_batch)?;
            sync_log |= ready.must_sync();
            self.unsynced_data_size += self.write_batch.data_size() as u64;
            r!(self.db.write(&self.write_batch));
            self.write_batch.clear();
            self.node.mut_store().post_ready(context);
            self.node.advance_append_async(ready);
        }
        if self.unsynced_data_size >= 512 * 1024
            || self.unsynced_data_size > 0
                && before_ready
                    .checked_duration_since(self.last_sync_time)
                    .map_or(false, |d| d >= SYNC_INTERVAL)
        {
            sync_log = true;
        }
        if sync_log {
            r!(self.db.sync_wal());
            let after_sync = Instant::now();
            if let Some(d) = after_sync.checked_duration_since(before_ready) {
                if d >= Duration::from_millis(100) {
                    info!(self.logger, "syncing WAL takes {:?}", d);
                }
            }
            self.last_sync_time = after_sync;
            self.unsynced_data_size = 0;
            self.node.on_persist_ready(self.last_ready_number);
            self.has_ready = true;
            let mut persisted_messages = std::mem::take(&mut self.persisted_messages);
            for msgs in persisted_messages.drain(..) {
                self.send_messages(msgs);
            }
            self.persisted_messages = persisted_messages;
        }
        Ok(())
    }

    fn suggest_timeout(&self) -> Option<Duration> {
        if self.unsynced_data_size > 0 {
            Some(
                match Instant::now().checked_duration_since(self.last_sync_time) {
                    Some(dur) if dur < SYNC_INTERVAL => SYNC_INTERVAL - dur,
                    _ => Duration::new(0, 0),
                },
            )
        } else {
            None
        }
    }

    pub fn poll(&mut self) -> Result<()> {
        let mut timeout = None;
        loop {
            let mut msg = if let Some(dur) = timeout {
                match self.receiver.recv_timeout(dur) {
                    Ok(msg) => Some(msg),
                    Err(RecvTimeoutError::Timeout) => None,
                    Err(RecvTimeoutError::Disconnected) => return Ok(()),
                }
            } else {
                match self.receiver.recv() {
                    Ok(msg) => Some(msg),
                    Err(_) => return Ok(()),
                }
            };
            while let Some(m) = msg {
                self.process(m);
                if self.abort {
                    return Ok(());
                }
                if self.processed_msg_cnt >= 4096 {
                    if self.has_ready {
                        break;
                    }
                    self.processed_msg_cnt = 0;
                }
                msg = self.receiver.try_recv().ok();
            }
            if self.has_ready {
                self.process_ready()?;
            }
            timeout = self.suggest_timeout();
        }
    }
}
