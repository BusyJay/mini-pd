use bytes::{BufMut, Bytes, BytesMut};
use crossbeam::channel;
use futures::{
    channel::mpsc::{self, Receiver, Sender},
    StreamExt,
};
use futures_timer::Delay;
use kvproto::{
    metapb,
    pdpb::{self, Member, RegionHeartbeatRequest, RegionHeartbeatResponse, StoreStats},
};
use parking_lot::Mutex;
use protobuf::Message;
use slog::{error, info, Logger};
use std::{
    collections::HashMap,
    convert::TryInto,
    mem,
    sync::{
        atomic::{AtomicU64, AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};
use yatp::{task::future::TaskCell, Remote};

use crate::cluster::events::RegionEvent;
use crate::{kv, Command, Error, Event, Msg, Res, Result};

use super::codec::*;
use super::{events::RegionEventListeners, stats::RegionStats};

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

async fn bootstrap(cluster: Cluster) {
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

async fn reload_cluser_meta(cluster: Cluster) {
    let (tx, mut rx) = mpsc::channel(1);
    let mut last_term = 0;
    loop {
        let msg = Msg::WaitEvent {
            event: Event::CommittedToCurrentTermAsLeader,
            notifier: tx.clone(),
        };
        cluster.sender.send(msg).unwrap();
        let term = match rx.next().await {
            Some(Res::RoleInfo { term, .. }) => term,
            _ => return,
        };
        if term == last_term {
            Delay::new(Duration::from_secs(10)).await;
            continue;
        }
        cluster.meta.stores.lock().clear();
        cluster.meta.regions.lock().clear();
        last_term = term;
    }
}

pub struct ClusterMeta {
    id: AtomicU64,
    bootstrap: AtomicU8,
    regions: Mutex<HashMap<u64, RegionStats>>,
    stores: Mutex<HashMap<u64, StoreStats>>,
    region_caches: Mutex<HashMap<u64, metapb::Region>>,
    store_scheduler: Mutex<HashMap<u64, Sender<RegionHeartbeatResponse>>>,
    region_event_hub: Mutex<HashMap<u64, RegionEventListeners>>,
}

impl ClusterMeta {
    pub fn id(&self) -> u64 {
        // Order doesn't matter.
        self.id.load(Ordering::Relaxed)
    }

    pub fn is_bootstrapped(&self) -> bool {
        self.bootstrap.load(Ordering::Relaxed) == BOOTSTRAPPED
    }

    pub fn stores(&self) -> &Mutex<HashMap<u64, StoreStats>> {
        &self.stores
    }

    pub fn regions(&self) -> &Mutex<HashMap<u64, RegionStats>> {
        &self.regions
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
        let kvs = vec![
            (CLUSTER_BOOTSTRAP_KEY, buffer.into()),
            (
                Bytes::copy_from_slice(&region_key(region.get_id())),
                region.write_length_delimited_to_bytes().unwrap().into(),
            ),
        ];
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
    sender: channel::Sender<Msg>,
    logger: Logger,
}

impl Cluster {
    pub fn new(sender: channel::Sender<Msg>, remote: &Remote<TaskCell>, logger: Logger) -> Cluster {
        let cluster = Cluster {
            meta: Arc::new(ClusterMeta {
                id: AtomicU64::new(0),
                bootstrap: AtomicU8::new(NOT_BOOTSTRAP),
                regions: Default::default(),
                stores: Default::default(),
                region_caches: Default::default(),
                store_scheduler: Default::default(),
                region_event_hub: Default::default(),
            }),
            sender,
            logger,
        };
        let c = cluster.clone();
        remote.spawn(async move { bootstrap(c).await });
        let c = cluster.clone();
        remote.spawn(async move { reload_cluser_meta(c).await });
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
        let key = Bytes::copy_from_slice(&store_key(store.get_id()));
        let value = store.write_to_bytes().unwrap().into();
        let put = Command::put(key, value);
        let msg = Msg::command(put, Some(tx.clone()));
        self.sender.send(msg).unwrap();
        let res = rx.next().await;
        if !matches!(res, Some(Res::Success)) {
            return Err(Error::Other(format!("failed to put store: {:?}", res)));
        }
        Ok(())
    }

    pub fn update_store_stats(&self, stats: pdpb::StoreStats) {
        self.meta.stores.lock().insert(stats.get_store_id(), stats);
    }

    pub fn stores(&self) -> &Mutex<HashMap<u64, StoreStats>> {
        self.meta.stores()
    }

    pub fn regions(&self) -> &Mutex<HashMap<u64, RegionStats>> {
        self.meta.regions()
    }

    pub fn register_region_stream(
        &self,
        remote: &Remote<TaskCell>,
        store_id: u64,
        mut rx: Receiver<RegionHeartbeatRequest>,
        tx: Sender<RegionHeartbeatResponse>,
    ) {
        let meta = self.meta.clone();
        let sender = self.sender.clone();
        let loop_update_region = async move {
            let mut batch = Vec::with_capacity(100);
            let mut updates = vec![];
            match rx.next().await {
                Some(r) => batch.push(r),
                None => return,
            };
            while let Ok(Some(r)) = rx.try_next() {
                batch.push(r);
                if batch.len() == 100 {
                    break;
                }
            }
            let mut region_cached = meta.region_caches.lock();
            let mut stats = meta.regions.lock();
            for heartbeat in &mut batch {
                let region_id = heartbeat.get_region().get_id();
                let stats = stats.entry(region_id).or_default();
                if stats.term > heartbeat.get_term() {
                    continue;
                }
                let mut hub = meta.region_event_hub.lock();
                let mut listeners = hub.get_mut(&region_id);
                let region = heartbeat.take_region();
                let cached_region = region_cached.get(&region_id);
                if cached_region.map_or(true, |r| stale_region(r, &region)) {
                    updates.push((
                        Bytes::copy_from_slice(&region_key(region.get_id())),
                        region.write_to_bytes().unwrap().into(),
                    ));
                    if let Some(listeners) = &mut listeners {
                        for mut l in mem::take(&mut listeners.region_changed) {
                            let _ = l.try_send(RegionEvent::RegionChanged {
                                region: region.clone(),
                            });
                        }
                    }
                    if let Some(r) = &cached_region {
                        let origin_ver = r.get_region_epoch().get_version();
                        let cur_ver = region.get_region_epoch().get_version();
                        if origin_ver != cur_ver {
                            let origin_key = region_range_key(r.get_end_key(), origin_ver);
                            updates.push((origin_key, Bytes::new()));
                            let cur_key = region_range_key(region.get_end_key(), cur_ver);
                            let val = region_range_value(region_id);
                            updates.push((cur_key, val));
                        }
                    }
                    region_cached.insert(region.get_id(), region);
                }
                stats.refresh_with(heartbeat, listeners);
            }
            drop(region_cached);
            drop(stats);
            batch.clear();
            // This is not technically correct as it doesn't check if caches match physical
            // storage. But it will eventually correct as heartbeat will keep being reported.
            if !updates.is_empty() {
                let cmd = Command::batch_put(updates);
                let _ = sender.try_send(Msg::command(cmd, None));
            }
        };
        remote.spawn(loop_update_region);
        self.meta.store_scheduler.lock().insert(store_id, tx);
    }

    pub fn put_regions(&self, regions: Vec<metapb::Region>) {
        let mut updates = Vec::with_capacity(regions.len());
        let meta = &self.meta;
        let mut cached = meta.region_caches.lock();
        for region in regions {
            let region_id = region.get_id();
            let mut hub = meta.region_event_hub.lock();
            let mut listeners = hub.get_mut(&region_id);
            let cached_region = cached.get(&region_id);
            if cached_region.map_or(true, |r| stale_region(r, &region)) {
                updates.push((
                    Bytes::copy_from_slice(&region_key(region.get_id())),
                    region.write_to_bytes().unwrap().into(),
                ));
                if let Some(listeners) = &mut listeners {
                    for mut l in mem::take(&mut listeners.region_changed) {
                        let _ = l.try_send(RegionEvent::RegionChanged {
                            region: region.clone(),
                        });
                    }
                }
                if let Some(r) = &cached_region {
                    let origin_ver = r.get_region_epoch().get_version();
                    let cur_ver = region.get_region_epoch().get_version();
                    if origin_ver != cur_ver {
                        let origin_key = region_range_key(r.get_end_key(), origin_ver);
                        updates.push((origin_key, Bytes::new()));
                        let cur_key = region_range_key(region.get_end_key(), cur_ver);
                        let val = region_range_value(region_id);
                        updates.push((cur_key, val));
                    }
                }
                cached.insert(region.get_id(), region);
            }
        }
        if !updates.is_empty() {
            let cmd = Command::batch_put(updates);
            let _ = self.sender.try_send(Msg::command(cmd, None));
        }
    }
}

fn stale_region(lhs: &metapb::Region, rhs: &metapb::Region) -> bool {
    let lhs_epoch = lhs.get_region_epoch();
    let rhs_epoch = rhs.get_region_epoch();
    lhs_epoch.get_version() < rhs_epoch.get_version()
        || lhs_epoch.get_conf_ver() < rhs_epoch.get_conf_ver()
}
