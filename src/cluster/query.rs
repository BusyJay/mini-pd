use std::convert::TryInto;

use kvproto::metapb;
use protobuf::Message;
use rocksdb::{DBIterator, ReadOptions, SeekKey, DB};

use super::codec::{region_key, region_range_key, store_key};
use crate::kv::RockSnapshot;

pub fn get_region_by_key(snap: &RockSnapshot, key: &[u8], prev: bool) -> Option<metapb::Region> {
    let mut iter = snap.iter();
    let b = if !prev {
        let start_key = region_range_key(key, 0);
        iter.seek(SeekKey::Key(&start_key)).unwrap()
    } else {
        let start_key = region_range_key(key, u64::MAX);
        iter.seek_for_prev(SeekKey::Key(&start_key)).unwrap()
    };
    if !b {
        return None;
    }
    let id = u64::from_be_bytes(iter.value().try_into().unwrap());
    get_region_by_id(snap, id)
}

pub fn get_region_by_id(snap: &RockSnapshot, id: u64) -> Option<metapb::Region> {
    let key = region_key(id);
    let val = snap.get(&key).unwrap()?;
    let mut r = metapb::Region::default();
    r.merge_from_bytes(&val).unwrap();
    Some(r)
}

pub fn scan_region(snap: &RockSnapshot, start: &[u8], end: &[u8]) -> Vec<metapb::Region> {
    let mut iter = snap.iter();
    let start_key = region_range_key(start, 0);
    let mut regions = vec![];
    if !iter.seek(SeekKey::Key(&start_key)).unwrap() {
        return regions;
    }
    loop {
        let id = u64::from_be_bytes(iter.value().try_into().unwrap());
        if let Some(r) = get_region_by_id(snap, id) {
            if r.get_start_key() < end || end.is_empty() {
                regions.push(r);
            } else {
                break;
            }
        }
        if !iter.next().unwrap() {
            break;
        }
    }
    regions
}

pub fn load_store(snap: &RockSnapshot, id: u64) -> Option<metapb::Store> {
    let key = store_key(id);
    let mut store = metapb::Store::default();
    let v = snap.get(&key).unwrap()?;
    store.merge_from_bytes(&v).unwrap();
    Some(store)
}

fn iter_all_store(snap: &RockSnapshot) -> DBIterator<&DB> {
    let end_key = store_key(u64::MAX);
    let mut opt = ReadOptions::default();
    opt.set_iterate_upper_bound(end_key.to_vec());
    snap.iter_opt(opt)
}

pub fn load_all_stores(snap: &RockSnapshot) -> Vec<metapb::Store> {
    let mut iter = iter_all_store(snap);
    let mut stores = Vec::with_capacity(3);
    let start_key = store_key(0);
    if iter.seek(SeekKey::Key(&start_key)).unwrap() {
        loop {
            let mut store = metapb::Store::default();
            store.merge_from_bytes(iter.value()).unwrap();
            stores.push(store);
            if !iter.next().unwrap() {
                break;
            }
        }
    }
    stores
}

pub fn get_cluster_version(snap: &RockSnapshot) -> Option<String> {
    let mut iter = iter_all_store(snap);
    let start_key = store_key(0);
    if iter.seek(SeekKey::Key(&start_key)).unwrap() {
        loop {
            let mut store = metapb::Store::default();
            store.merge_from_bytes(iter.value()).unwrap();
            if store.get_state() != metapb::StoreState::Tombstone {
                // TODO: correct way should use lease version.
                return Some(store.take_version());
            }
            if !iter.next().unwrap() {
                break;
            }
        }
    }
    None
}
