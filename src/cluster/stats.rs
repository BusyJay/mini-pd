use kvproto::{metapb, pdpb};

#[derive(Default, Clone)]
pub struct RegionStats {
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub keys_written: u64,
    pub keys_read: u64,
    pub approximate_size: u64,
    pub approximate_keys: u64,
    pub term: u64,
    pub leader: Option<metapb::Peer>,
    pub down_peer: Vec<pdpb::PeerStats>,
    pub pending_peer: Vec<metapb::Peer>,
}
