use std::mem;

use kvproto::{
    metapb,
    pdpb::{self, RegionHeartbeatRequest},
};

use super::events::{RegionEvent, RegionEventListeners};

#[derive(Default, Clone)]
pub struct RegionStats {
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub keys_written: u64,
    pub keys_read: u64,
    pub approximate_size: u64,
    pub approximate_keys: u64,
    pub term: u64,
    pub leader: metapb::Peer,
    pub down_peers: Vec<pdpb::PeerStats>,
    pub pending_peers: Vec<metapb::Peer>,
}

impl RegionStats {
    pub fn refresh_with(
        &mut self,
        heartbeat: &mut RegionHeartbeatRequest,
        listeners: Option<&mut RegionEventListeners>,
    ) {
        self.bytes_written = heartbeat.get_bytes_written();
        self.bytes_read = heartbeat.get_bytes_read();
        self.keys_written = heartbeat.get_keys_written();
        self.keys_read = heartbeat.get_keys_read();
        self.approximate_size = heartbeat.get_approximate_size();
        self.approximate_keys = heartbeat.get_approximate_keys();
        self.term = heartbeat.get_term();
        if self.leader != *heartbeat.get_leader() {
            self.leader = heartbeat.take_leader();
            if let Some(listeners) = listeners {
                for mut l in mem::take(&mut listeners.change_leader) {
                    let _ = l.try_send(RegionEvent::LeaderChanged {
                        leader: self.leader.clone(),
                        term: self.term,
                    });
                }
            }
        }
        self.down_peers = heartbeat.take_down_peers().into();
        self.pending_peers = heartbeat.take_pending_peers().into();
    }
}
