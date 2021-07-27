use std::{
    collections::HashMap,
    convert::TryInto,
    sync::{
        atomic::{AtomicU64, AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};

use bytes::{BufMut, Bytes, BytesMut};
use crossbeam::channel::Sender;
use futures::{channel::mpsc, StreamExt};
use futures_timer::Delay;
use kvproto::{
    metapb,
    pdpb::{self, Member},
};
use parking_lot::Mutex;
use protobuf::Message;
use rocksdb::{ReadOptions, SeekKey, DB};
use slog::{error, info, Logger};
use yatp::{task::future::TaskCell, Remote};

use crate::{kv, Command, Error, Event, Msg, Res, Result};

use super::stats::{Region, Store};

const CLUSTER_ID_KEY: Bytes = Bytes::from_static(b"dcluster");
const CLUSTER_BOOTSTRAP_KEY: Bytes = Bytes::from_static(b"dcluster_bootstrap");

static REGION_KEY_PREFIX: &[u8] = b"dr";
static STORE_KEY_PREFIX: &[u8] = b"ds";

const NOT_BOOTSTRAP: u8 = 0x01;
pub const BOOTSTRAPPING: u8 = 0x02;
pub const BOOTSTRAPPED: u8 = 0x03;

fn new_member(id: u64, addr: String) -> Member {
    let mut member = Member::default();
    member.set_member_id(id);
    member.mut_peer_urls().push(addr.clone());
    member.mut_client_urls().push(addr);
    member
}

fn region_key(id: u64) -> Bytes {
    kv::combine_key(REGION_KEY_PREFIX, id)
}

fn store_key(id: u64) -> Bytes {
    kv::combine_key(STORE_KEY_PREFIX, id)
}

pub fn load_store(db: &DB, id: u64, store: &mut metapb::Store) -> Result<()> {
    let key = store_key(id);
    match kv::get_msg(db, &key)? {
        Some(s) => {
            *store = s;
            Ok(())
        }
        None => Err(Error::Other("store not found".to_string())),
    }
}

pub fn load_all_stores(db: &DB) -> Result<Vec<metapb::Store>> {
    let start_key = store_key(0);
    let end_key = store_key(u64::MAX);
    let mut opt = ReadOptions::default();
    opt.set_iterate_upper_bound(end_key.to_vec());
    let mut iter = db.iter_opt(opt);
    let mut stores = Vec::with_capacity(3);
    if iter.seek(SeekKey::Key(&start_key)).unwrap() {
        loop {
            let mut store = metapb::Store::default();
            store.merge_from_bytes(iter.value())?;
            stores.push(store);
            if !iter.next().unwrap() {
                break;
            }
        }
    }
    Ok(stores)
}

async fn bootstrap(cluster: &Cluster) {
    let (tx, mut rx) = mpsc::channel(1);
    loop {
        let msg = Msg::WaitEvent {
            event: Event::CommittedToCurrentTerm,
            notifier: tx.clone(),
        };
        cluster.sender.send(msg).unwrap();
        let (leader, term, my_id) = match rx.next().await {
            Some(Res::RoleInfo {
                leader,
                term,
                my_id,
            }) => (leader, term, my_id),
            _ => return,
        };
        let msg = Msg::snapshot(tx.clone());
        cluster.sender.send(msg).unwrap();
        match rx.next().await {
            Some(Res::Snapshot(snap)) => match snap.get(&*CLUSTER_ID_KEY) {
                Ok(Some(value)) => {
                    let id = u64::from_le_bytes((&*value).try_into().unwrap());
                    let bootstrapped = match snap.get(&*CLUSTER_BOOTSTRAP_KEY) {
                        Ok(Some(_)) => {
                            cluster.meta.bootstrap.store(BOOTSTRAPPED, Ordering::SeqCst);
                            true
                        }
                        Ok(None) => false,
                        Err(e) => panic!("unable to get bootstrap state: {}", e),
                    };
                    info!(
                        cluster.logger,
                        "recover cluster id {}, bootstrap: {}", id, bootstrapped
                    );
                    cluster.meta.id.store(id, Ordering::SeqCst);
                    return;
                }
                Ok(None) => {}
                Err(e) => {
                    panic!("unable to fetch cluster id: {}", e);
                }
            },
            Some(Res::Fail(e)) => {
                error!(cluster.logger, "failed to fetch cluster id: {}", e);
                Delay::new(Duration::from_secs(1)).await;
                continue;
            }
            _ => return,
        }
        if leader != my_id {
            Delay::new(Duration::from_secs(1)).await;
            continue;
        }
        let id: u64 = rand::random();
        let mut buf = BytesMut::with_capacity(8);
        buf.put_u64_le(id);
        let put = Command::put(CLUSTER_ID_KEY, buf.freeze());
        cluster
            .sender
            .send(Msg::check_term_command(put, term, Some(tx.clone())))
            .unwrap();
        match rx.next().await {
            Some(Res::Success) => {
                info!(cluster.logger, "init cluster with id {}", id);
                cluster.meta.id.store(id, Ordering::Relaxed);
                return;
            }
            Some(Res::Fail(reason)) => {
                error!(
                    cluster.logger,
                    "failed to initialize cluster id: {}", reason
                );
                Delay::new(Duration::from_secs(1)).await;
                continue;
            }
            _ => return,
        }
    }
}

pub struct ClusterMeta {
    id: AtomicU64,
    bootstrap: AtomicU8,
    regions: Arc<Mutex<HashMap<u64, Region>>>,
    stores: Arc<Mutex<HashMap<u64, Store>>>,
}

impl ClusterMeta {
    pub fn id(&self) -> u64 {
        // Order doesn't matter.
        self.id.load(Ordering::Relaxed)
    }

    pub fn is_bootstrapped(&self) -> bool {
        self.bootstrap.load(Ordering::Relaxed) == BOOTSTRAPPED
    }

    pub fn stores(&self) -> Arc<Mutex<HashMap<u64, Store>>> {
        self.stores.clone()
    }

    pub fn regions(&self) -> Arc<Mutex<HashMap<u64, Region>>> {
        self.regions.clone()
    }
}

pub struct BootstrapGuard {
    cluster: Cluster,
    reset_on_drop: bool,
}

impl BootstrapGuard {
    pub async fn bootstrap_with(
        &mut self,
        store: &metapb::Store,
        region: &metapb::Region,
    ) -> Result<()> {
        info!(
            self.cluster.logger,
            "{:?} tries to bootstrap with region: {:?}", store, region
        );
        let (tx, mut rx) = mpsc::channel(1);
        let mut buffer = store.write_length_delimited_to_bytes()?;
        region.write_length_delimited_to_vec(&mut buffer)?;
        let mut kvs = Vec::with_capacity(2);
        kvs.push((CLUSTER_BOOTSTRAP_KEY, buffer.into()));
        kvs.push((
            region_key(region.get_id()),
            region.write_length_delimited_to_bytes().unwrap().into(),
        ));
        let command = Command::batch_put(kvs);
        let msg = Msg::command(command, Some(tx));
        self.cluster.sender.send(msg).unwrap();
        match rx.next().await {
            Some(Res::Success) => {
                self.reset_on_drop = false;
                Ok(())
            }
            Some(Res::Fail(e)) => Err(Error::Other(e)),
            None => Err(Error::Other("instance shutting down".to_string())),
            res => panic!("unexpected result: {:?}", res),
        }
    }
}

impl Drop for BootstrapGuard {
    fn drop(&mut self) {
        if self.reset_on_drop {
            self.cluster
                .meta
                .bootstrap
                .store(NOT_BOOTSTRAP, Ordering::SeqCst);
        }
    }
}

#[derive(Clone)]
pub struct Cluster {
    meta: Arc<ClusterMeta>,
    sender: Sender<Msg>,
    logger: Logger,
}

impl Cluster {
    pub fn new(sender: Sender<Msg>, remote: &Remote<TaskCell>, logger: Logger) -> Cluster {
        let cluster = Cluster {
            meta: Arc::new(ClusterMeta {
                id: AtomicU64::new(0),
                bootstrap: AtomicU8::new(NOT_BOOTSTRAP),
                regions: Arc::default(),
                stores: Arc::default(),
            }),
            sender,
            logger,
        };
        let c = cluster.clone();
        remote.spawn(async move { bootstrap(&c).await });
        cluster
    }

    pub fn id(&self) -> u64 {
        self.meta.id()
    }

    pub fn meta(&self) -> &Arc<ClusterMeta> {
        &self.meta
    }

    pub fn is_bootstrapped(&self) -> bool {
        self.meta.is_bootstrapped()
    }

    pub fn lock_for_bootstrap(&self) -> std::result::Result<BootstrapGuard, u8> {
        if self.meta.id.load(Ordering::SeqCst) == 0 {
            return Err(BOOTSTRAPPING);
        }
        let mut bootstrap_state = self.meta.bootstrap.load(Ordering::SeqCst);
        loop {
            if bootstrap_state != NOT_BOOTSTRAP {
                return Err(bootstrap_state);
            }
            match self.meta.bootstrap.compare_exchange_weak(
                NOT_BOOTSTRAP,
                BOOTSTRAPPING,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return Ok(BootstrapGuard {
                        cluster: self.clone(),
                        reset_on_drop: true,
                    })
                }
                Err(s) => bootstrap_state = s,
            }
        }
    }

    pub async fn get_members(&self) -> Result<(Member, Vec<Member>)> {
        let (tx, mut rx) = mpsc::channel(1);
        let msg = Msg::WaitEvent {
            event: Event::CommittedToCurrentTerm,
            notifier: tx.clone(),
        };
        self.sender.send(msg).unwrap();
        let leader = match rx.next().await {
            Some(Res::RoleInfo { leader, .. }) => leader,
            res => {
                return Err(Error::Other(format!(
                    "failed to get member list: {:?}",
                    res
                )))
            }
        };
        let snap = Msg::snapshot(tx.clone());
        self.sender.send(snap).unwrap();
        let snap = match rx.next().await {
            Some(Res::Snapshot(s)) => s,
            res => return Err(Error::Other(format!("failed to get snap: {:?}", res))),
        };
        let ids = kv::load_replica_ids(&snap)?;
        let leader_addr = kv::load_address(&snap, leader);
        let leader = new_member(leader, leader_addr);
        let members = ids
            .into_iter()
            .map(|id| {
                let addr = kv::load_address(&snap, id);
                new_member(id, addr)
            })
            .collect();
        Ok((leader, members))
    }

    pub async fn put_store(&self, store: metapb::Store) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        // TODO: check address, state.
        let key = store_key(store.get_id());
        let value = store.write_to_bytes().unwrap().into();
        let put = Command::put(key, value);
        let msg = Msg::command(put, Some(tx.clone()));
        self.sender.send(msg).unwrap();
        let res = rx.next().await;
        if !matches!(res, Some(Res::Success)) {
            return Err(Error::Other(format!("failed to put store: {:?}", res)));
        }
        let mut guard = self.meta.stores.lock();
        let s = guard.entry(store.id).or_default();
        s.store = store;
        Ok(())
    }

    pub fn update_store_stats(&self, stats: pdpb::StoreStats) {
        let mut guard = self.meta.stores.lock();
        if let Some(s) = guard.get_mut(&stats.get_store_id()) {
            s.stats = stats;
        }
    }

    pub fn get_cluster_version(&self) -> Option<String> {
        let guard = self.meta.stores.lock();
        if guard.is_empty() {
            return None;
        }
        let store = guard.iter().next().unwrap().1;
        Some(store.store.get_version().to_string())
    }

    pub fn stores(&self) -> Arc<Mutex<HashMap<u64, Store>>> {
        self.meta.stores()
    }

    pub fn regions(&self) -> Arc<Mutex<HashMap<u64, Region>>> {
        self.meta.regions()
    }
}
