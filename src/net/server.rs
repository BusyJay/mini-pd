use super::service::{PdService, RaftService};
use crate::allocator::Allocator;
use crate::cluster::Cluster;
use crate::kv::{AddressMap, Fsm, Msg, RaftClient};
use crate::{Config, Error, Result};
use crossbeam::channel::Sender;
use grpcio::{EnvBuilder, Environment};
use kvproto::{minipdpb, pdpb};
use rocksdb::DB;
use slog::Logger;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use yatp::task::future::TaskCell;
use yatp::ThreadPool;

pub struct FsmHandle {
    id: u64,
    sender: Sender<Msg>,
    db: Arc<DB>,
    env: Arc<Environment>,
    thread: JoinHandle<()>,
}

impl FsmHandle {
    pub fn sender(&self) -> &Sender<Msg> {
        &self.sender
    }
}

pub struct Server {
    logger: Logger,
    address_map: AddressMap,
    pool: ThreadPool<TaskCell>,
    config: Config,
    handle: Option<FsmHandle>,
    server: Option<grpcio::Server>,
}

impl Server {
    pub fn new(address_map: AddressMap, config: Config, logger: Logger) -> Server {
        Server {
            logger,
            address_map,
            config,
            pool: yatp::Builder::new("futures").build_future_pool(),
            handle: None,
            server: None,
        }
    }

    pub fn start(&mut self) -> Result<()> {
        if self.handle.is_some() {
            return Err(Error::Other("server has been started".to_owned()));
        }
        let raft_env = Arc::new(
            EnvBuilder::new()
                .name_prefix("grpc-raft")
                .cq_count(1)
                .build(),
        );
        let remote = self.pool.remote();
        self.address_map
            .lock()
            .insert(self.config.my_id, self.config.advertise_address.clone());
        let raft_client = RaftClient::new(
            raft_env.clone(),
            self.address_map.clone(),
            remote.clone(),
            self.logger.clone(),
        );
        let mut fsm = Fsm::new(&self.config, raft_client, &self.logger, remote.clone())?;
        let sender = fsm.sender();
        let id = fsm.id();
        let db = fsm.db();
        let thread = thread::Builder::new()
            .name("raft".to_owned())
            .spawn(move || {
                if let Err(e) = fsm.poll() {
                    panic!("poll failure: {}", e);
                }
            })
            .unwrap();
        self.handle = Some(FsmHandle {
            id,
            sender,
            db,
            env: raft_env,
            thread,
        });
        self.start_grpc_server()?;
        Ok(())
    }

    fn get_bind_pair(&self) -> Result<(String, u16)> {
        if let Some(p) = self.config.address.find(':') {
            let host = self.config.address[..p].to_owned();
            if let Ok(port) = self.config.address[p + 1..].parse() {
                return Ok((host, port));
            }
        }
        Err(Error::Other(format!(
            "invalid address {}",
            self.config.address
        )))
    }

    fn start_grpc_server(&mut self) -> Result<()> {
        let handle = self.handle.as_ref().unwrap();
        let raft_service = RaftService::new(handle.id, handle.sender.clone(), self.logger.clone());
        let raft_service = minipdpb::create_mini_pd_raft(raft_service);

        let tso = Allocator::new(
            handle.sender.clone(),
            self.pool.remote(),
            self.logger.clone(),
        );
        let cluster = Cluster::new(
            handle.sender.clone(),
            self.pool.remote(),
            self.logger.clone(),
        );
        let pd_service = PdService::new(
            tso,
            cluster,
            handle.db.clone(),
            self.pool.remote().clone(),
            self.logger.clone(),
        );
        let pd_service = pdpb::create_pd(pd_service);

        let (host, port) = self.get_bind_pair()?;
        let mut server = grpcio::ServerBuilder::new(handle.env.clone())
            // Maybe it's a better idea to use different thread for raft and pd.
            .register_service(raft_service)
            .register_service(pd_service)
            .bind(host, port)
            .build()?;
        server.start();
        self.server = Some(server);
        Ok(())
    }

    pub fn sender(&self) -> &Sender<Msg> {
        self.handle.as_ref().unwrap().sender()
    }

    pub fn advertise_address(&self) -> &str {
        &self.config.advertise_address
    }

    pub fn shutdown(&mut self) {
        match self.server.take() {
            Some(mut s) => {
                s.shutdown();
            }
            None => return,
        }
        let handle = match self.handle.take() {
            Some(h) => h,
            None => return,
        };
        let _ = handle.sender.send(Msg::Stop);
        handle.thread.join().unwrap();
    }
}
