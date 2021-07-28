use futures::channel::mpsc::Sender;
use kvproto::metapb;

pub enum RegionEvent {
    LeaderChanged { leader: metapb::Peer, term: u64 },
    RegionChanged { region: metapb::Region },
}

pub struct RegionEventListeners {
    pub change_leader: Vec<Sender<RegionEvent>>,
    pub region_changed: Vec<Sender<RegionEvent>>,
}
