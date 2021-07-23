use futures::channel::mpsc::{self, Receiver, Sender};
use futures::prelude::*;
use futures_timer::Delay;
use grpcio::{ChannelBuilder, Environment, WriteFlags};
use kvproto::minipdpb::*;
use parking_lot::Mutex;
use raft::eraftpb::Message;
use slog::{error, info, o, Logger};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use yatp::task::future::TaskCell;
use yatp::Remote;

pub type AddressMap = Arc<Mutex<HashMap<u64, String>>>;

pub struct Connection {
    address_map: AddressMap,
    id: u64,
    logger: Logger,
    msgs: Receiver<Message>,
    env: Arc<Environment>,
}

impl Connection {
    async fn poll(&mut self) {
        loop {
            let addr_opt = self.address_map.lock().get(&self.id).cloned();
            let addr = match addr_opt {
                Some(a) => a,
                None => {
                    // backoff.
                    Delay::new(Duration::from_secs(1)).await;
                    if let Ok(None) = self.msgs.try_next() {
                        return;
                    }
                    continue;
                }
            };
            let conn = ChannelBuilder::new(self.env.clone()).connect(&addr);
            if !conn.wait_for_connected(Duration::from_secs(3)).await {
                error!(self.logger, "failed to connect {} after 3 second", addr);
                continue;
            }

            info!(
                self.logger,
                "new connection to {} with address {}", self.id, addr
            );
            let client = MiniPdRaftClient::new(conn);
            let (mut tx, rx) = client.raft().unwrap();
            let mut msg_stream = (&mut self.msgs).map(|m| Ok((m, WriteFlags::default())));
            let (send_res, recv_res) = futures::join!(tx.send_all(&mut msg_stream), rx);
            match (send_res, recv_res) {
                (Ok(_), Ok(_)) => return,
                (Err(e), Ok(_)) | (Ok(_), Err(e)) => {
                    error!(self.logger, "failed to send message: {}", e)
                }
                (Err(e1), Err(e2)) => error!(self.logger, "failed to send message: {} {}", e1, e2),
            }
        }
    }
}

pub struct RaftClient {
    connections: HashMap<u64, Sender<Message>>,
    address_map: AddressMap,
    env: Arc<Environment>,
    logger: Logger,
    pool: Remote<TaskCell>,
}

impl RaftClient {
    pub fn new(
        env: Arc<Environment>,
        address_map: AddressMap,
        pool: Remote<TaskCell>,
        logger: Logger,
    ) -> RaftClient {
        RaftClient {
            connections: HashMap::default(),
            address_map,
            env,
            logger,
            pool,
        }
    }

    pub fn send(&mut self, msg: Message) -> Option<Message> {
        let to = msg.get_to();
        if let Some(sender) = self.connections.get_mut(&to) {
            return sender.try_send(msg).err().map(|e| e.into_inner());
        }
        let (mut tx, rx) = mpsc::channel(40960);
        tx.try_send(msg).unwrap();
        self.connections.insert(to, tx);
        let mut conn = Connection {
            address_map: self.address_map.clone(),
            id: to,
            logger: self.logger.new(o!("conn_to" => to)),
            msgs: rx,
            env: self.env.clone(),
        };
        self.pool.spawn(async move { conn.poll().await });
        None
    }

    pub fn address_map(&self) -> &AddressMap {
        &self.address_map
    }
}
