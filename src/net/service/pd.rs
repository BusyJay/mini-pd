use crate::allocator::{self, Allocator};
use crate::cluster::{self, Cluster, ClusterMeta, BOOTSTRAPPING};
use crate::Error;
use futures::channel::mpsc;
use futures::{join, prelude::*};
use grpcio::{DuplexSink, WriteFlags};
use grpcio::{RequestStream, RpcContext, UnarySink};
use kvproto::pdpb::*;
use rocksdb::DB;
use slog::{error, Logger};
use std::cmp;
use std::sync::Arc;
use yatp::task::future::TaskCell;
use yatp::Remote;

fn new_tso_response(cluster_id: u64, count: u64, start: &mut u64) -> TsoResponse {
    let mut resp = TsoResponse::default();
    if fill_header_raw(resp.mut_header(), cluster_id) {
        resp.set_count(count as u32);
        allocator::fill_timestamp(*start, resp.mut_timestamp());
    }
    *start += count;
    resp
}

fn fill_header_raw(header: &mut ResponseHeader, cluster_id: u64) -> bool {
    if cluster_id != 0 {
        header.set_cluster_id(cluster_id);
        return true;
    }
    fill_error(
        header,
        ErrorType::UNKNOWN,
        "cluster id not set yet".to_string(),
    );
    false
}

fn fill_header(header: &mut ResponseHeader, meta: &ClusterMeta) -> bool {
    let cluster_id = meta.id();
    fill_header_raw(header, cluster_id)
}

fn fill_error(header: &mut ResponseHeader, et: ErrorType, msg: String) {
    header.mut_error().set_field_type(et);
    header.mut_error().set_message(msg);
}

macro_rules! check_cluster {
    ($ctx:expr, $cluster:expr, $sink:ident, $req:ident, $resp:expr) => {{
        let id = $cluster.id();
        if id == 0 {
            let mut resp = $resp;
            fill_header_raw(resp.mut_header(), 0);
            $ctx.spawn(async move {
                let _ = $sink.success(resp).await;
            });
            return;
        }
        let request_id = $req.get_header().get_cluster_id();
        let mut resp = $resp;
        resp.mut_header().set_cluster_id(id);
        if request_id != 0 && request_id != id {
            fill_error(
                resp.mut_header(),
                ErrorType::UNKNOWN,
                format!("cluster id not match, your {}, my {}", request_id, id),
            );
            $ctx.spawn(async move {
                let _ = $sink.success(resp).await;
            });
            return;
        }
        resp
    }};
}

macro_rules! check_bootstrap {
    ($ctx:expr, $cluster:expr, $sink:ident, $req:ident, $resp:expr) => {{
        let mut resp = check_cluster!($ctx, $cluster, $sink, $req, $resp);
        if !$cluster.is_bootstrapped() {
            fill_error(
                resp.mut_header(),
                ErrorType::NOT_BOOTSTRAPPED,
                String::new(),
            );
            $ctx.spawn(async move {
                let _ = $sink.success(resp).await;
            });
            return;
        }
        resp
    }};
}

#[derive(Clone)]
pub struct PdService {
    allocator: Allocator,
    cluster: Cluster,
    db: Arc<DB>,
    remote: Remote<TaskCell>,
    logger: Logger,
}

impl PdService {
    pub fn new(
        allocator: Allocator,
        cluster: Cluster,
        db: Arc<DB>,
        remote: Remote<TaskCell>,
        logger: Logger,
    ) -> PdService {
        PdService {
            allocator,
            cluster,
            remote,
            db,
            logger,
        }
    }
}

impl Pd for PdService {
    fn get_members(
        &mut self,
        ctx: ::grpcio::RpcContext,
        req: GetMembersRequest,
        sink: ::grpcio::UnarySink<GetMembersResponse>,
    ) {
        let mut resp = check_cluster!(ctx, self.cluster, sink, req, GetMembersResponse::default());
        let cluster = self.cluster.clone();
        let f = async move {
            match cluster.get_members().await {
                Ok((leader, peers)) => {
                    resp.set_leader(leader.clone());
                    resp.set_etcd_leader(leader.clone());
                    resp.set_members(peers.into());
                }
                Err(e) => {
                    fill_error(resp.mut_header(), ErrorType::UNKNOWN, format!("{}", e));
                }
            }
            let _ = sink.success(resp).await;
        };
        ctx.spawn(f);
    }

    fn tso(
        &mut self,
        ctx: RpcContext,
        stream: RequestStream<TsoRequest>,
        mut sink: DuplexSink<TsoResponse>,
    ) {
        let allocator = self.allocator.tso().clone();
        let logger = self.logger.clone();
        let meta = self.cluster.meta().clone();
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
                    buf.clear();
                    let count = match batch_rx.next().await {
                        Some(r) => cmp::max(r.get_count() as u64, 1),
                        None => {
                            sink.close().await?;
                            return Ok::<_, Error>(());
                        }
                    };
                    let mut sum = count;
                    while buf.len() < 100 {
                        if let Ok(Some(r)) = batch_rx.try_next() {
                            let c = cmp::max(r.get_count() as u64, 1);
                            sum += c;
                            buf.push(c);
                        } else {
                            break;
                        }
                    }
                    let ts = match allocator.alloc(sum).await {
                        Ok(t) => t,
                        Err(e) => {
                            for i in 0..buf.len() + 1 {
                                let mut resp = TsoResponse::default();
                                let header = resp.mut_header();
                                fill_header(header, &meta);
                                if !header.has_error() {
                                    fill_error(header, ErrorType::UNKNOWN, format!("{}", e));
                                }
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
                    let cluster_id = meta.id();
                    let resp = new_tso_response(cluster_id, count, &mut start);
                    sink.send((resp, WriteFlags::default().buffer_hint(!buf.is_empty())))
                        .await?;
                    for (i, c) in buf.iter().enumerate() {
                        let resp = new_tso_response(cluster_id, *c, &mut start);
                        sink.send((resp, WriteFlags::default().buffer_hint(i + 1 != buf.len())))
                            .await?;
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
        req: BootstrapRequest,
        sink: ::grpcio::UnarySink<BootstrapResponse>,
    ) {
        let mut resp = check_cluster!(ctx, self.cluster, sink, req, BootstrapResponse::default());
        let mut guard = match self.cluster.lock_for_bootstrap() {
            Ok(guard) => guard,
            Err(e) => {
                let (et, msg) = if e == BOOTSTRAPPING {
                    (ErrorType::UNKNOWN, "cluster is still being bootstrapped")
                } else {
                    (ErrorType::ALREADY_BOOTSTRAPPED, "cluster was bootstrapped")
                };
                fill_error(resp.mut_header(), et, msg.to_string());
                ctx.spawn(async move {
                    let _ = sink.success(resp).await;
                });
                return;
            }
        };
        let logger = self.logger.clone();
        let f = async move {
            if let Err(e) = guard
                .bootstrap_with(req.get_store(), req.get_region())
                .await
            {
                error!(logger, "failed to bootstrap cluster: {}", e);
                fill_error(resp.mut_header(), ErrorType::UNKNOWN, format!("{}", e));
            }
            let _ = sink.success(resp).await;
        };
        ctx.spawn(f);
    }

    fn is_bootstrapped(
        &mut self,
        ctx: RpcContext,
        req: IsBootstrappedRequest,
        sink: UnarySink<IsBootstrappedResponse>,
    ) {
        let mut resp = check_cluster!(
            ctx,
            self.cluster,
            sink,
            req,
            IsBootstrappedResponse::default()
        );
        let bootstrapped = self.cluster.is_bootstrapped();
        resp.set_bootstrapped(bootstrapped);
        ctx.spawn(async move {
            let _ = sink.success(resp).await;
        });
    }

    fn alloc_id(
        &mut self,
        ctx: ::grpcio::RpcContext,
        req: AllocIDRequest,
        sink: ::grpcio::UnarySink<AllocIDResponse>,
    ) {
        let mut resp = check_cluster!(ctx, self.cluster, sink, req, AllocIDResponse::default());
        let id = self.allocator.id().clone();
        let f = async move {
            match id.alloc(1).await {
                Ok(id) => resp.set_id(id),
                Err(e) => {
                    fill_error(resp.mut_header(), ErrorType::UNKNOWN, format!("{}", e));
                }
            }
            let _ = sink.success(resp).await;
        };
        ctx.spawn(f);
    }

    fn get_store(
        &mut self,
        ctx: ::grpcio::RpcContext,
        req: GetStoreRequest,
        sink: ::grpcio::UnarySink<GetStoreResponse>,
    ) {
        let mut resp = check_bootstrap!(ctx, self.cluster, sink, req, GetStoreResponse::default());
        let store_id = req.get_store_id();
        if let Err(e) = cluster::load_store(&self.db, store_id, resp.mut_store()) {
            fill_error(resp.mut_header(), ErrorType::UNKNOWN, format!("{}", e));
        }
        ctx.spawn(async move {
            let _ = sink.success(resp);
        });
    }

    fn put_store(
        &mut self,
        ctx: ::grpcio::RpcContext,
        mut req: PutStoreRequest,
        sink: ::grpcio::UnarySink<PutStoreResponse>,
    ) {
        let mut resp = check_bootstrap!(ctx, self.cluster, sink, req, PutStoreResponse::default());
        let cluster = self.cluster.clone();
        let store = req.take_store();
        let f = async move {
            if let Err(e) = cluster.put_store(store).await {
                fill_error(resp.mut_header(), ErrorType::UNKNOWN, format!("{}", e));
            }
            let _ = sink.success(resp).await;
        };
        ctx.spawn(f);
    }
    fn get_all_stores(
        &mut self,
        ctx: ::grpcio::RpcContext,
        req: GetAllStoresRequest,
        sink: ::grpcio::UnarySink<GetAllStoresResponse>,
    ) {
        let mut resp = check_bootstrap!(
            ctx,
            self.cluster,
            sink,
            req,
            GetAllStoresResponse::default()
        );
        match cluster::load_all_stores(&self.db) {
            Ok(stores) => resp.set_stores(stores.into()),
            Err(e) => fill_error(resp.mut_header(), ErrorType::UNKNOWN, format!("{}", e)),
        }
        ctx.spawn(async move {
            let _ = sink.success(resp);
        });
    }

    fn store_heartbeat(
        &mut self,
        ctx: ::grpcio::RpcContext,
        mut req: StoreHeartbeatRequest,
        sink: ::grpcio::UnarySink<StoreHeartbeatResponse>,
    ) {
        let mut resp = check_bootstrap!(
            ctx,
            self.cluster,
            sink,
            req,
            StoreHeartbeatResponse::default()
        );
        self.cluster.update_store_stats(req.take_stats());
        // TODO: support cluster version.
        if let Some(version) = self.cluster.get_cluster_version() {
            resp.set_cluster_version(version);
        }
        ctx.spawn(async move {
            let _ = sink.success(resp);
        });
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
