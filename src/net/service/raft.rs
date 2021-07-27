use crate::kv::Msg;
use futures::prelude::*;
use grpcio::{ClientStreamingSink, RequestStream, RpcContext, RpcStatus, RpcStatusCode};
use kvproto::minipdpb::*;
use raft::eraftpb::Message;
use slog::{error, Logger};

#[derive(Clone)]
pub struct RaftService {
    id: u64,
    sender: crossbeam::channel::Sender<Msg>,
    logger: Logger,
}

impl RaftService {
    pub fn new(id: u64, sender: crossbeam::channel::Sender<Msg>, logger: Logger) -> RaftService {
        RaftService { id, sender, logger }
    }
}

impl MiniPdRaft for RaftService {
    fn raft(
        &mut self,
        ctx: RpcContext,
        mut stream: RequestStream<Message>,
        sink: ClientStreamingSink<Empty>,
    ) {
        let my_id = self.id;
        let logger = self.logger.clone();
        let sender = self.sender.clone();
        let f = async move {
            let mut err = None;
            loop {
                match stream.try_next().await {
                    Ok(Some(msg)) => {
                        if msg.get_to() != my_id {
                            let message = format!(
                                "message sent to wrong target, my: {}, expect: {}, from: {}",
                                my_id,
                                msg.get_to(),
                                msg.get_from()
                            );
                            err = Some(RpcStatus::with_message(RpcStatusCode::NOT_FOUND, message));
                            break;
                        }
                        if let Err(e) = sender.send(Msg::RaftMessage(msg)) {
                            let message = format!("can't dispatch raft message: {}", e);
                            err = Some(RpcStatus::with_message(RpcStatusCode::UNKNOWN, message));
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        let message = format!("failed to receive message: {}", e);
                        err = Some(RpcStatus::with_message(RpcStatusCode::CANCELLED, message));
                        break;
                    }
                }
            }
            let res = match err {
                None => sink.success(Empty::default()).await,
                Some(e) => {
                    error!(logger, "failed to receive message {}", e.message());
                    sink.fail(e).await
                }
            };
            if let Err(e) = res {
                error!(logger, "failed to respond: {}", e);
            }
        };
        ctx.spawn(f);
    }
}
