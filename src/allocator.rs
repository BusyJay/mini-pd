mod id;
mod tso;

use crossbeam::channel::Sender;
use slog::Logger;
use yatp::{task::future::TaskCell, Remote};

use crate::Msg;

#[derive(Clone)]
pub struct Allocator {
    id: id::IdAllocator,
    tso: tso::TsoAllocator,
}

impl Allocator {
    pub fn new(sender: Sender<Msg>, remote: &Remote<TaskCell>, logger: Logger) -> Allocator {
        let tso = tso::TsoAllocator::new(sender.clone(), remote, logger.clone());
        let id = id::IdAllocator::new(sender, remote, logger);

        Allocator { id, tso }
    }

    pub fn id(&self) -> &id::IdAllocator {
        &self.id
    }

    pub fn tso(&self) -> &tso::TsoAllocator {
        &self.tso
    }
}

pub use tso::fill_timestamp;
