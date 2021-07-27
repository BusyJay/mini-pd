use kvproto::{metapb, pdpb};
use parking_lot::Mutex;
use std::sync::Arc;

pub struct PeerStats {
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub keys_written: u64,
    pub keys_read: u64,
    pub approximate_size: u64,
    pub approximate_keys: u64,
    pub term: u64,
}

pub enum PeerState {
    Health,
    Pending,
    Down,
}

pub struct Peer {
    pub peer: metapb::Peer,
    pub stats: PeerStats,
    pub state: PeerState,
}

#[derive(Default)]
pub struct Store {
    pub peers: Vec<Arc<Mutex<Peer>>>,
    pub store: metapb::Store,
    pub stats: pdpb::StoreStats,
}

pub struct Region {
    pub region: metapb::Region,
    pub leader: Option<metapb::Peer>,
    pub term: u64,
    pub peers: Vec<Arc<Mutex<Peer>>>,
}
