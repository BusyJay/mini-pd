use mini_pd::{AddressMap, Config, Server};
use parking_lot::Mutex;
use slog::Logger;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tempdir::TempDir;

static PORT: AtomicUsize = AtomicUsize::new(1234);

pub struct Cluster {
    _data_dir: Vec<TempDir>,
    pub servers: Vec<Server>,
}

impl Cluster {
    pub fn new(count: u64, initial_count: u64, logger: Logger) -> Cluster {
        let data_dir = (0..count)
            .map(|id| TempDir::new(&format!("mini-pd-{}", id)).unwrap())
            .collect();
        let mut peers = Vec::default();
        let mut map = HashMap::default();
        for id in 1..=initial_count {
            peers.push(id);
            map.insert(
                id,
                format!("127.0.0.1:{}", PORT.fetch_add(1, Ordering::SeqCst)),
            );
        }
        let servers = (1..=count)
            .zip(&data_dir)
            .map(|(id, p): (_, &TempDir)| {
                let (my_addr, map) = if id <= initial_count {
                    (
                        map.get(&id).unwrap().clone(),
                        Arc::new(Mutex::new(map.clone())),
                    )
                } else {
                    (
                        format!("127.0.0.1:{}", PORT.fetch_add(1, Ordering::SeqCst)),
                        AddressMap::default(),
                    )
                };
                let mut config = Config::default();
                config.my_id = id;
                config.address = my_addr.clone();
                config.advertise_address = my_addr.clone();
                config.data_dir = p.path().to_path_buf();
                config.initial_peers = peers.clone();
                config.initial_address_book.insert(1, my_addr.clone());
                config.raft_election_ticks = 5;
                config.raft_heartbeat_ticks = 1;
                Server::new(map, config, logger.clone())
            })
            .collect();
        Cluster {
            _data_dir: data_dir,
            servers,
        }
    }

    pub fn start(&mut self) {
        for server in &mut self.servers {
            server.start().unwrap();
        }
    }

    pub fn server(&self, id: u64) -> &Server {
        &self.servers[id as usize - 1]
    }

    fn shutdown(&mut self) {
        for server in &mut self.servers {
            server.shutdown();
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.shutdown();
    }
}
