use std::cmp;

use crate::kv::Msg;
use crate::tso::TsoAllocator;
use crate::Error;
use futures::channel::mpsc;
use futures::{join, prelude::*};
use grpcio::{ClientStreamingSink, RequestStream, RpcContext, RpcStatus, RpcStatusCode};
use grpcio::{DuplexSink, WriteFlags};
use kvproto::minipdpb::*;
use kvproto::pdpb::*;
use raft::eraftpb::Message;
use slog::{error, Logger};
use yatp::task::future::TaskCell;
use yatp::Remote;

#[derive(Clone)]
pub struct PdService {
    tso: TsoAllocator,
    remote: Remote<TaskCell>,
    logger: Logger,
}

impl PdService {
    pub fn new(tso: TsoAllocator, remote: Remote<TaskCell>, logger: Logger) -> PdService {
        PdService {
            tso,
            remote,
            logger,
        }
    }
}

impl Pd for PdService {
    fn get_members(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: GetMembersRequest,
        sink: ::grpcio::UnarySink<GetMembersResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }

    fn tso(
        &mut self,
        ctx: RpcContext,
        stream: RequestStream<TsoRequest>,
        mut sink: DuplexSink<TsoResponse>,
    ) {
        let allocator = self.tso.clone();
        let logger = self.logger.clone();
        let f = async move {
            let (batch_tx, mut batch_rx) = mpsc::channel(100);
            let collect = async move {
                let mut wrap_stream = stream.map_err(Error::Rpc);
                let mut wrap_tx =
                    batch_tx.sink_map_err(|e| Error::Other(format!("failed to forward: {}", e)));
                wrap_tx.send_all(&mut wrap_stream).await
            };
            let mut buf = Vec::with_capacity(100);
            let batch_process = async {
                loop {
                    let req = match batch_rx.next().await {
                        Some(r) => r,
                        None => {
                            sink.close().await?;
                            return Ok::<_, Error>(());
                        }
                    };
                    let mut sum = cmp::max(req.get_count() as u64, 1);
                    while let Ok(Some(r)) = batch_rx.try_next() {
                        sum += cmp::max(r.get_count() as u64, 1);
                        buf.push(r);
                    }
                    let ts = match allocator.alloc(sum).await {
                        Ok(t) => t,
                        Err(e) => {
                            for i in 0..buf.len() + 1 {
                                let mut resp = TsoResponse::default();
                                resp.mut_header()
                                    .mut_error()
                                    .set_field_type(ErrorType::UNKNOWN);
                                resp.mut_header().mut_error().set_message(format!("{}", e));
                                sink.send((
                                    resp,
                                    WriteFlags::default().buffer_hint(i != buf.len()),
                                ))
                                .await?;
                            }
                            continue;
                        }
                    };
                    let mut start = ts - sum + 1;
                    let mut resp = TsoResponse::default();
                    resp.set_count(req.get_count());
                    resp.mut_timestamp().set_logical(start as i64);
                    resp.mut_timestamp().set_physical(start as i64);
                    sink.send((resp, WriteFlags::default().buffer_hint(!buf.is_empty())))
                        .await?;
                    start += cmp::max(req.get_count() as u64, 1);
                    for (i, req) in buf.iter().enumerate() {
                        let mut resp = TsoResponse::default();
                        resp.set_count(req.get_count());
                        resp.mut_timestamp().set_logical(start as i64);
                        resp.mut_timestamp().set_physical(start as i64);
                        sink.send((resp, WriteFlags::default().buffer_hint(i + 1 != buf.len())))
                            .await?;
                        start += cmp::max(req.get_count() as u64, 1);
                    }
                }
            };
            let res = join!(collect, batch_process);
            if res.0.is_err() || res.1.is_err() {
                error!(logger, "failed to handle tso: {:?}", res);
            }
        };
        ctx.spawn(f);
    }

    fn bootstrap(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: BootstrapRequest,
        sink: ::grpcio::UnarySink<BootstrapResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn is_bootstrapped(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: IsBootstrappedRequest,
        sink: ::grpcio::UnarySink<IsBootstrappedResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn alloc_id(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: AllocIDRequest,
        sink: ::grpcio::UnarySink<AllocIDResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn get_store(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: GetStoreRequest,
        sink: ::grpcio::UnarySink<GetStoreResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn put_store(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: PutStoreRequest,
        sink: ::grpcio::UnarySink<PutStoreResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn get_all_stores(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: GetAllStoresRequest,
        sink: ::grpcio::UnarySink<GetAllStoresResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn store_heartbeat(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: StoreHeartbeatRequest,
        sink: ::grpcio::UnarySink<StoreHeartbeatResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn region_heartbeat(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _stream: ::grpcio::RequestStream<RegionHeartbeatRequest>,
        sink: ::grpcio::DuplexSink<RegionHeartbeatResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn get_region(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: GetRegionRequest,
        sink: ::grpcio::UnarySink<GetRegionResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn get_prev_region(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: GetRegionRequest,
        sink: ::grpcio::UnarySink<GetRegionResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn get_region_by_id(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: GetRegionByIDRequest,
        sink: ::grpcio::UnarySink<GetRegionResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn scan_regions(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: ScanRegionsRequest,
        sink: ::grpcio::UnarySink<ScanRegionsResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn ask_split(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: AskSplitRequest,
        sink: ::grpcio::UnarySink<AskSplitResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn report_split(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: ReportSplitRequest,
        sink: ::grpcio::UnarySink<ReportSplitResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn ask_batch_split(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: AskBatchSplitRequest,
        sink: ::grpcio::UnarySink<AskBatchSplitResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn report_batch_split(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: ReportBatchSplitRequest,
        sink: ::grpcio::UnarySink<ReportBatchSplitResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn get_cluster_config(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: GetClusterConfigRequest,
        sink: ::grpcio::UnarySink<GetClusterConfigResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn put_cluster_config(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: PutClusterConfigRequest,
        sink: ::grpcio::UnarySink<PutClusterConfigResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn scatter_region(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: ScatterRegionRequest,
        sink: ::grpcio::UnarySink<ScatterRegionResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn get_gc_safe_point(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: GetGCSafePointRequest,
        sink: ::grpcio::UnarySink<GetGCSafePointResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn update_gc_safe_point(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: UpdateGCSafePointRequest,
        sink: ::grpcio::UnarySink<UpdateGCSafePointResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn update_service_gc_safe_point(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: UpdateServiceGCSafePointRequest,
        sink: ::grpcio::UnarySink<UpdateServiceGCSafePointResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn sync_regions(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _stream: ::grpcio::RequestStream<SyncRegionRequest>,
        sink: ::grpcio::DuplexSink<SyncRegionResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn get_operator(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: GetOperatorRequest,
        sink: ::grpcio::UnarySink<GetOperatorResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn sync_max_ts(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: SyncMaxTSRequest,
        sink: ::grpcio::UnarySink<SyncMaxTSResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn split_regions(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: SplitRegionsRequest,
        sink: ::grpcio::UnarySink<SplitRegionsResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn get_dc_location_info(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _req: GetDCLocationInfoRequest,
        sink: ::grpcio::UnarySink<GetDCLocationInfoResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
}

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
