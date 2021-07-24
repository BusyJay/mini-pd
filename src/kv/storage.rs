use super::AddressMap;
use crate::r;
use kvproto::metapb::{self, Peer, PeerRole};
use kvproto::raft_serverpb::{RaftApplyState, RaftLocalState, RegionLocalState};
use protobuf::Message;
use raft::eraftpb::{ConfState, Entry, Snapshot};
use raft::prelude::*;
use raft::{Error, Result, StorageError};
use rocksdb::{DBOptions, Writable, WriteBatch, WriteOptions, DB};
use std::fs;
use std::path::Path;
use std::sync::Arc;

pub static RAFT_LOG_PREFIX_KEY: u8 = b'l';
// Uses a larger prefix to avoid potential overlaps in log gc.
pub static RAFT_STATE_KEY: &[u8] = b"m";
pub static APPLY_STATE_KEY: &[u8] = b"o";
pub static REGION_STATE_KEY: &[u8] = b"r";
pub static ADDRESS_PREFIX_KEY: u8 = b'a';
pub static DATA_PREFIX_KEY: u8 = b'd';

const INIT_TERM: u64 = 3;
const INIT_INDEX: u64 = 3;

pub fn log_key(index: u64) -> [u8; 9] {
    let mut buf = [RAFT_LOG_PREFIX_KEY; 9];
    buf[1..].copy_from_slice(&index.to_be_bytes());
    buf
}

pub fn address_key(id: u64) -> [u8; 9] {
    let mut address = [ADDRESS_PREFIX_KEY; 9];
    address[1..].copy_from_slice(&id.to_be_bytes());
    address
}

pub fn valid_data_key(key: &[u8]) -> bool {
    !key.is_empty() && key[0] == DATA_PREFIX_KEY
}

fn get_msg<T: Message + Default>(db: &DB, key: &[u8]) -> Result<Option<T>> {
    let val = match db.get(&key) {
        Ok(Some(v)) => v,
        Ok(None) => return Ok(None),
        Err(e) => return Err(Error::Store(StorageError::Other(e.into()))),
    };
    let mut t = T::default();
    t.merge_from_bytes(&*val)?;
    Ok(Some(t))
}

fn must_get_msg<T: Message + Default>(db: &DB, key: &[u8]) -> Result<T> {
    match get_msg(db, key)? {
        Some(v) => Ok(v),
        None => Err(Error::Store(StorageError::Unavailable)),
    }
}

fn conf_state_from_region(region: &metapb::Region) -> ConfState {
    let mut conf_state = ConfState::default();
    let mut in_joint = false;
    for p in region.get_peers() {
        match p.get_role() {
            PeerRole::Voter => {
                conf_state.mut_voters().push(p.get_id());
                conf_state.mut_voters_outgoing().push(p.get_id());
            }
            PeerRole::Learner => conf_state.mut_learners().push(p.get_id()),
            role => {
                in_joint = true;
                match role {
                    PeerRole::IncomingVoter => conf_state.mut_voters().push(p.get_id()),
                    PeerRole::DemotingVoter => {
                        conf_state.mut_voters_outgoing().push(p.get_id());
                        conf_state.mut_learners_next().push(p.get_id());
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
    if !in_joint {
        conf_state.mut_voters_outgoing().clear();
    }
    conf_state
}

pub fn exists(path: impl AsRef<Path>) -> bool {
    let p = path.as_ref();
    p.exists() && fs::read_dir(p).unwrap().next().is_some()
}

pub fn bootstrap(
    path: impl AsRef<Path>,
    address_map: &AddressMap,
    peers: &[u64],
    my_id: u64,
) -> crate::Result<()> {
    let p = path.as_ref();
    if exists(p) {
        return Err(crate::Error::Storage(format!(
            "{} already exists",
            p.display()
        )));
    }
    if my_id == 0 {
        return Err(crate::Error::Other("my id can't be 0".to_owned()));
    }
    if !peers.is_empty() && !peers.contains(&my_id) {
        return Err(crate::Error::Other(format!(
            "my id {} not in peers {:?}",
            my_id, peers
        )));
    }
    struct Guard<'a>(bool, &'a Path);
    impl<'a> Drop for Guard<'a> {
        fn drop(&mut self) {
            if self.0 {
                let _ = std::fs::remove_dir_all(self.1);
            }
        }
    }
    let (init_term, init_index) = if peers.is_empty() {
        (0, 0)
    } else {
        (INIT_TERM, INIT_INDEX)
    };
    let mut guard = Guard(true, p);
    let mut opt = DBOptions::default();
    opt.create_if_missing(true);
    let db = r!(DB::open(opt, p.to_str().unwrap()));
    let wb = WriteBatch::new();

    let mut raft_state = RaftLocalState::default();
    raft_state.mut_hard_state().set_term(init_term);
    raft_state.mut_hard_state().set_commit(init_index);
    raft_state.set_last_index(init_index);
    r!(wb.put(RAFT_STATE_KEY, &raft_state.write_to_bytes().unwrap()));

    let mut replica_state = RegionLocalState::default();
    let members = replica_state.mut_region().mut_peers();
    for id in peers {
        let mut peer = Peer::default();
        peer.set_id(*id);
        members.push(peer);
    }
    r!(wb.put(REGION_STATE_KEY, &replica_state.write_to_bytes().unwrap()));

    let mut apply_state = RaftApplyState::default();
    apply_state.mut_truncated_state().set_index(init_index);
    apply_state.mut_truncated_state().set_term(init_term);
    apply_state.set_applied_index(init_index);
    r!(wb.put(APPLY_STATE_KEY, &apply_state.write_to_bytes().unwrap()));

    for (id, address) in &*address_map.lock() {
        r!(wb.put(&address_key(*id), address.as_bytes()));
    }

    let mut write_opts = WriteOptions::default();
    write_opts.set_sync(true);
    r!(db.write_opt(&wb, &write_opts));

    guard.0 = false;

    Ok(())
}

pub type RockSnapshot = rocksdb::rocksdb::Snapshot<Arc<DB>>;

/// Returned by `RockStorage::handle_raft_ready`, used for recording changed status of
/// `RaftLocalState` and `RaftApplyState`.
pub struct InvokeContext {
    /// Changed RaftLocalState is stored into `raft_state`.
    pub raft_state: RaftLocalState,
    /// Changed RaftApplyState is stored into `apply_state`.
    pub apply_state: RaftApplyState,
    /// If the ready has new entries.
    pub has_new_entries: bool,
    /// The old region is stored here if there is a snapshot.
    pub replica_state: Option<RegionLocalState>,
    last_term: u64,
}

impl InvokeContext {
    pub fn new(store: &RockStorage) -> InvokeContext {
        InvokeContext {
            raft_state: store.raft_state.clone(),
            apply_state: store.apply_state.clone(),
            has_new_entries: false,
            replica_state: None,
            last_term: store.last_term,
        }
    }
}

pub struct RockStorage {
    db: Arc<DB>,
    id: u64,
    raft_state: RaftLocalState,
    replica_state: RegionLocalState,
    apply_state: RaftApplyState,
    last_term: u64,
}

impl RockStorage {
    pub fn open(path: impl AsRef<Path>, id: u64) -> crate::Result<RockStorage> {
        let p = path.as_ref();
        if !p.exists() {
            return Err(crate::Error::Storage(format!("{} not exists", p.display())));
        }
        let mut opts = DBOptions::default();
        opts.create_if_missing(false);
        let db = match DB::open(opts, p.to_str().unwrap()) {
            Ok(db) => Arc::new(db),
            Err(e) => return Err(crate::Error::Storage(e)),
        };
        let raft_state: RaftLocalState = get_msg(&db, RAFT_STATE_KEY)?.unwrap_or_default();
        let apply_state = get_msg(&db, APPLY_STATE_KEY)?.unwrap_or_default();
        let replica_state = get_msg(&db, REGION_STATE_KEY)?.unwrap_or_default();
        let last_index = raft_state.get_last_index();
        let last_term = if last_index == INIT_INDEX {
            INIT_TERM
        } else if last_index == 0 {
            0
        } else {
            must_get_msg::<Entry>(&db, &log_key(last_index))?.get_term()
        };

        Ok(RockStorage {
            db,
            id,
            raft_state,
            replica_state,
            apply_state,
            last_term,
        })
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn db(&self) -> Arc<DB> {
        self.db.clone()
    }

    pub fn rock_snapshot(&self) -> RockSnapshot {
        RockSnapshot::new(self.db.clone())
    }

    pub fn applied(&self) -> u64 {
        self.apply_state.get_applied_index()
    }

    pub fn singleton(&self) -> bool {
        let members = self.replica_state.get_region().get_peers();
        members.len() == 1 && members[0].get_id() == self.id
    }

    pub fn append(
        &mut self,
        invoke_ctx: &mut InvokeContext,
        entries: Vec<Entry>,
        batch: &mut WriteBatch,
    ) -> crate::Result<u64> {
        let prev_last_index = invoke_ctx.raft_state.get_last_index();
        if entries.is_empty() {
            return Ok(prev_last_index);
        }

        invoke_ctx.has_new_entries = true;

        let (last_index, last_term) = {
            let e = entries.last().unwrap();
            (e.get_index(), e.get_term())
        };

        for e in &entries {
            r!(batch.put(&log_key(e.get_index()), &e.write_to_bytes().unwrap()));
        }
        // Delete any previously appended log entries which never committed.
        if last_index < prev_last_index {
            for i in last_index + 1..=prev_last_index {
                r!(batch.delete(&log_key(i)));
            }
        }

        invoke_ctx.raft_state.set_last_index(last_index);
        invoke_ctx.last_term = last_term;

        Ok(last_index)
    }

    fn apply_snapshot(&mut self, _snapshot: &Snapshot, _batch: &mut WriteBatch) {
        unimplemented!()
    }

    fn truncated_index(&self) -> u64 {
        self.apply_state.get_truncated_state().get_index()
    }

    fn truncated_term(&self) -> u64 {
        self.apply_state.get_truncated_state().get_term()
    }

    pub fn process_ready(
        &mut self,
        invoke_ctx: &mut InvokeContext,
        ready: &mut Ready,
        batch: &mut WriteBatch,
    ) -> crate::Result<()> {
        if !ready.entries().is_empty() {
            self.append(invoke_ctx, ready.take_entries(), batch)?;
        }
        if invoke_ctx.apply_state.get_applied_index() != self.apply_state.get_applied_index() {
            r!(batch.put(
                APPLY_STATE_KEY,
                &invoke_ctx.apply_state.write_to_bytes().unwrap()
            ));
        }
        if !ready.snapshot().is_empty() {
            self.apply_snapshot(ready.snapshot(), batch);
        }
        if let Some(hs) = ready.hs() {
            invoke_ctx.raft_state.set_hard_state(hs.clone());
            r!(batch.put(
                RAFT_STATE_KEY,
                &invoke_ctx.raft_state.write_to_bytes().unwrap()
            ));
        }
        Ok(())
    }

    pub fn post_ready(&mut self, mut invoke_ctx: InvokeContext) {
        if let Some(ctx) = invoke_ctx.replica_state.take() {
            self.replica_state = ctx;
        }
        self.apply_state = invoke_ctx.apply_state;
        self.raft_state = invoke_ctx.raft_state;
        self.last_term = invoke_ctx.last_term;
    }
}

impl raft::Storage for RockStorage {
    /// `initial_state` is called when Raft is initialized. This interface will return a `RaftState`
    /// which contains `HardState` and `ConfState`.
    ///
    /// `RaftState` could be initialized or not. If it's initialized it means the `Storage` is
    /// created with a configuration, and its last index and term should be greater than 0.
    fn initial_state(&self) -> Result<RaftState> {
        let hs = self.raft_state.get_hard_state().clone();
        let cs = conf_state_from_region(self.replica_state.get_region());
        Ok(RaftState::new(hs, cs))
    }

    /// Returns a slice of log entries in the range `[low, high)`.
    /// max_size limits the total size of the log entries returned if not `None`, however
    /// the slice of entries returned will always have length at least 1 if entries are
    /// found in the range.
    ///
    /// # Panics
    ///
    /// Panics if `high` is higher than `Storage::last_index(&self) + 1`.
    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>> {
        let limit = max_size.into();
        let mut fetch_size = 0;
        let mut buf = Vec::with_capacity((high - low) as usize);
        for i in low..high {
            // There should be small log gap in most cases, so use point get to get
            // best performance.
            let e: Entry = must_get_msg(&self.db, &log_key(i))?;
            if let Some(l) = limit {
                fetch_size += e.compute_size() as u64;
                if fetch_size > l && !buf.is_empty() {
                    return Ok(buf);
                }
            }
            buf.push(e);
        }
        Ok(buf)
    }

    /// Returns the term of entry idx, which must be in the range
    /// [first_index()-1, last_index()]. The term of the entry before
    /// first_index is retained for matching purpose even though the
    /// rest of that entry may not be available.
    fn term(&self, idx: u64) -> Result<u64> {
        if idx == self.truncated_index() {
            return Ok(self.truncated_term());
        }
        if self.truncated_term() == self.last_term
            && idx > self.truncated_index()
            && idx <= self.raft_state.get_last_index()
        {
            return Ok(self.last_term);
        }
        let l: Entry = must_get_msg(&self.db, &log_key(idx))?;
        Ok(l.get_term())
    }

    /// Returns the index of the first log entry that is possible available via entries, which will
    /// always equal to `truncated index` plus 1.
    ///
    /// New created (but not initialized) `Storage` can be considered as truncated at 0 so that 1
    /// will be returned in this case.
    fn first_index(&self) -> Result<u64> {
        Ok(self.truncated_index() + 1)
    }

    /// The index of the last entry replicated in the `Storage`.
    fn last_index(&self) -> Result<u64> {
        Ok(self.raft_state.get_last_index())
    }

    /// Returns the most recent snapshot.
    ///
    /// If snapshot is temporarily unavailable, it should return SnapshotTemporarilyUnavailable,
    /// so raft state machine could know that Storage needs some time to prepare
    /// snapshot and call snapshot later.
    /// A snapshot's index must not less than the `request_index`.
    fn snapshot(&self, _request_index: u64) -> Result<Snapshot> {
        Err(Error::Store(StorageError::SnapshotTemporarilyUnavailable))
    }
}
