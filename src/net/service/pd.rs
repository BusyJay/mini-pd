use crate::allocator::{self, Allocator};
use crate::cluster::{self, Cluster, ClusterMeta, BOOTSTRAPPING};
use crate::kv::{RockSnapshot, RockSnapshotFactory};
use crate::Error;
use futures::channel::mpsc;
use futures::{join, prelude::*};
use grpcio::{DuplexSink, RpcStatus, RpcStatusCode, WriteFlags};
use grpcio::{RequestStream, RpcContext, UnarySink};
use kvproto::metapb;
use kvproto::pdpb::{self, *};
use rocksdb::DB;
use slog::{error, Logger};
use std::cmp;
use std::ops::Bound::*;
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

fn check_id(my_id: u64, req_header: &RequestHeader) -> Option<(ErrorType, String)> {
    if my_id == 0 {
        return Some((
            ErrorType::NOT_BOOTSTRAPPED,
            "still initializing cluster id".to_owned(),
        ));
    }
    let req_id = req_header.get_cluster_id();
    if req_id != 0 && req_id != my_id {
        return Some((
            ErrorType::UNKNOWN,
            format!("cluster id not match, req_id {}, pd_id {}", req_id, my_id),
        ));
    }
    None
}

macro_rules! check_cluster {
    ($ctx:expr, $cluster:expr, $sink:ident, $req:ident, $resp:ident) => {{
        let id = $cluster.id();
        let mut resp = $resp::default();
        resp.mut_header().set_cluster_id(id);
        if let Some((et, msg)) = check_id(id, $req.get_header()) {
            fill_error(resp.mut_header(), et, msg);
            $ctx.spawn(async move {
                let _ = $sink.success(resp).await;
            });
            return;
        }
        resp
    }};
}

macro_rules! check_bootstrap {
    ($ctx:expr, $cluster:expr, $sink:ident, $req:ident, $resp:ident) => {{
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

    fn get_region_by_id_impl(
        &self,
        ctx: RpcContext,
        id: Option<u64>,
        mut resp: GetRegionResponse,
        sink: UnarySink<GetRegionResponse>,
    ) {
        if let Some(id) = id {
            if cluster::load_region(&self.db, id, resp.mut_region()).is_ok() {
                if let Some(stats) = self.cluster.regions().lock().get(&id) {
                    resp.set_leader(stats.leader.clone());
                    resp.set_down_peers(stats.down_peers.clone().into());
                    resp.set_pending_peers(stats.pending_peers.clone().into());
                }
            }
        }
        if !resp.get_region().has_region_epoch() {
            fill_error(
                resp.mut_header(),
                ErrorType::REGION_NOT_FOUND,
                String::new(),
            );
        }
        ctx.spawn(async move {
            let _ = sink.success(resp);
        });
    }

    fn get_region_impl(
        &mut self,
        ctx: RpcContext,
        mut req: GetRegionRequest,
        sink: UnarySink<GetRegionResponse>,
        reverse: bool,
    ) {
        let resp = check_bootstrap!(ctx, self.cluster, sink, req, GetRegionResponse);
        let range_caches = self.cluster.range_caches().lock().clone();
        let res = if !reverse {
            range_caches
                .range((Excluded(req.take_region_key()), Unbounded))
                .next()
        } else {
            range_caches
                .range((Unbounded, Included(req.take_region_key())))
                .next_back()
        };
        self.get_region_by_id_impl(ctx, res.map(|r| r.1 .0), resp, sink);
    }

    fn get_split_id_count(&self, region: &metapb::Region, new_splits: u64) -> crate::Result<u64> {
        let meta = self.cluster.meta();
        let cached = meta.get_region_cache(region.get_id());
        if cached.as_ref().map_or(true, |r| r != region) {
            return Err(Error::Other(format!("stale region, my {:?}", cached)));
        }
        let count = (region.get_peers().len() as u64 + 1) * new_splits;
        Ok(count)
    }
}

impl Pd for PdService {
    fn get_members(
        &mut self,
        ctx: RpcContext,
        req: GetMembersRequest,
        sink: UnarySink<GetMembersResponse>,
    ) {
        let mut resp = check_cluster!(ctx, self.cluster, sink, req, GetMembersResponse);
        let cluster = self.cluster.clone();
        let f = async move {
            match cluster.get_members().await {
                Ok((leader, peers)) => {
                    resp.set_leader(leader.clone());
                    resp.set_etcd_leader(leader);
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
        ctx: RpcContext,
        req: BootstrapRequest,
        sink: UnarySink<BootstrapResponse>,
    ) {
        let mut resp = check_cluster!(ctx, self.cluster, sink, req, BootstrapResponse);
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
        let mut resp = check_cluster!(ctx, self.cluster, sink, req, IsBootstrappedResponse);
        let bootstrapped = self.cluster.is_bootstrapped();
        resp.set_bootstrapped(bootstrapped);
        ctx.spawn(async move {
            let _ = sink.success(resp).await;
        });
    }

    fn alloc_id(&mut self, ctx: RpcContext, req: AllocIDRequest, sink: UnarySink<AllocIDResponse>) {
        let mut resp = check_cluster!(ctx, self.cluster, sink, req, AllocIDResponse);
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
        ctx: RpcContext,
        req: GetStoreRequest,
        sink: UnarySink<GetStoreResponse>,
    ) {
        let mut resp = check_bootstrap!(ctx, self.cluster, sink, req, GetStoreResponse);
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
        ctx: RpcContext,
        mut req: PutStoreRequest,
        sink: UnarySink<PutStoreResponse>,
    ) {
        let mut resp = check_bootstrap!(ctx, self.cluster, sink, req, PutStoreResponse);
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
        ctx: RpcContext,
        req: GetAllStoresRequest,
        sink: UnarySink<GetAllStoresResponse>,
    ) {
        let mut resp = check_bootstrap!(ctx, self.cluster, sink, req, GetAllStoresResponse);
        match cluster::load_all_stores(&RockSnapshot::new(self.db.clone())) {
            Ok(stores) => resp.set_stores(stores.into()),
            Err(e) => fill_error(resp.mut_header(), ErrorType::UNKNOWN, format!("{}", e)),
        }
        ctx.spawn(async move {
            let _ = sink.success(resp);
        });
    }

    fn store_heartbeat(
        &mut self,
        ctx: RpcContext,
        mut req: StoreHeartbeatRequest,
        sink: UnarySink<StoreHeartbeatResponse>,
    ) {
        let mut resp = check_bootstrap!(ctx, self.cluster, sink, req, StoreHeartbeatResponse);
        self.cluster.update_store_stats(req.take_stats());
        // TODO: support cluster version.
        if let Ok(version) = cluster::get_cluster_version(&self.db.build()) {
            resp.set_cluster_version(version);
        }
        ctx.spawn(async move {
            let _ = sink.success(resp);
        });
    }

    fn region_heartbeat(
        &mut self,
        ctx: RpcContext,
        mut stream: RequestStream<RegionHeartbeatRequest>,
        mut sink: DuplexSink<RegionHeartbeatResponse>,
    ) {
        // TODO: check cluster id.
        let logger = self.logger.clone();
        let cluster = self.cluster.clone();
        let remote = self.remote.clone();
        let f = async move {
            let req = match stream.try_next().await {
                Ok(Some(req)) => req,
                res => {
                    error!(logger, "failed to receive first heartbeat: {:?}", res);
                    let _ = sink
                        .fail(RpcStatus::with_message(
                            RpcStatusCode::UNKNOWN,
                            format!("failed to receive heartbeat: {:?}", res),
                        ))
                        .await;
                    return;
                }
            };
            let id = cluster.id();
            if let Some((et, msg)) = check_id(id, req.get_header()) {
                let mut resp = RegionHeartbeatResponse::default();
                resp.mut_header().set_cluster_id(id);
                fill_error(resp.mut_header(), et, msg);
                let _ = sink.send((resp, WriteFlags::default())).await;
                let _ = sink.close().await;
                return;
            }

            let (mut batch_tx, batch_rx) = mpsc::channel(1024);
            let (sched_tx, sched_rx) = mpsc::channel::<RegionHeartbeatResponse>(1024);
            let store_id = req.get_leader().get_store_id();
            batch_tx.try_send(req).unwrap();
            let collect = async move {
                let mut wrap_stream = stream.map_err(Error::Rpc);
                let mut wrap_tx =
                    batch_tx.sink_map_err(|e| Error::Other(format!("failed to forward: {}", e)));
                wrap_tx.send_all(&mut wrap_stream).await
            };
            let sched = async move {
                let mut wrap_sched = sched_rx.map(|r| Ok((r, WriteFlags::default())));
                let mut wrap_sink =
                    sink.sink_map_err(|e| Error::Other(format!("failed to forward: {}", e)));
                wrap_sink.send_all(&mut wrap_sched).await
            };
            cluster.register_region_stream(&remote, store_id, batch_rx, sched_tx);
            let res = join!(collect, sched);
            if res.0.is_err() || res.1.is_err() {
                error!(logger, "failed to handle tso: {:?}", res);
            }
        };
        ctx.spawn(f);
    }

    fn get_region(
        &mut self,
        ctx: RpcContext,
        req: GetRegionRequest,
        sink: UnarySink<GetRegionResponse>,
    ) {
        self.get_region_impl(ctx, req, sink, false)
    }

    fn get_prev_region(
        &mut self,
        ctx: RpcContext,
        req: GetRegionRequest,
        sink: UnarySink<GetRegionResponse>,
    ) {
        self.get_region_impl(ctx, req, sink, true)
    }

    fn get_region_by_id(
        &mut self,
        ctx: RpcContext,
        req: GetRegionByIDRequest,
        sink: UnarySink<GetRegionResponse>,
    ) {
        let resp = check_bootstrap!(ctx, self.cluster, sink, req, GetRegionResponse);
        self.get_region_by_id_impl(ctx, Some(req.get_region_id()), resp, sink)
    }

    fn scan_regions(
        &mut self,
        ctx: RpcContext,
        mut req: ScanRegionsRequest,
        sink: UnarySink<ScanRegionsResponse>,
    ) {
        let mut resp = check_bootstrap!(ctx, self.cluster, sink, req, ScanRegionsResponse);
        let range_caches = self.cluster.range_caches().lock().clone();
        let ranges =
            range_caches.range((Excluded(req.take_start_key()), Included(req.take_end_key())));
        let mut regions = Vec::with_capacity(64);
        for (_, (id, _)) in ranges {
            let mut region = metapb::Region::default();
            if cluster::load_region(&self.db, *id, &mut region).is_ok() {
                regions.push(region);
            } else {
                break;
            }
        }
        if regions.is_empty() {
            fill_error(
                resp.mut_header(),
                ErrorType::REGION_NOT_FOUND,
                String::new(),
            );
        } else {
            let stats = self.cluster.regions().lock();
            for r in regions {
                let mut s = pdpb::Region::default();
                s.set_region(r.clone());
                if let Some(stats) = stats.get(&r.get_id()) {
                    s.set_leader(stats.leader.clone());
                    resp.mut_leaders().push(stats.leader.clone());
                    s.set_down_peers(stats.down_peers.clone().into());
                    s.set_pending_peers(stats.pending_peers.clone().into());
                }
                resp.mut_regions().push(s);
                resp.mut_region_metas().push(r);
            }
        }
        ctx.spawn(async move {
            let _ = sink.success(resp);
        });
    }

    fn ask_split(
        &mut self,
        ctx: RpcContext,
        req: AskSplitRequest,
        sink: UnarySink<AskSplitResponse>,
    ) {
        let mut resp = check_bootstrap!(ctx, self.cluster, sink, req, AskSplitResponse);
        let region = req.get_region();
        let count = match self.get_split_id_count(region, 1) {
            Ok(c) => c,
            Err(e) => {
                fill_error(resp.mut_header(), ErrorType::UNKNOWN, format!("{}", e));
                ctx.spawn(async move {
                    let _ = sink.success(resp).await;
                });
                return;
            }
        };
        let id = self.allocator.id().clone();
        let f = async move {
            match id.alloc(count).await {
                Ok(id) => {
                    let start_id = id - count + 1;
                    resp.set_new_region_id(start_id);
                    let new_peer_ids = resp.mut_new_peer_ids();
                    for i in start_id + 1..=id {
                        new_peer_ids.push(i);
                    }
                }
                Err(e) => {
                    fill_error(resp.mut_header(), ErrorType::UNKNOWN, format!("{}", e));
                }
            }
            let _ = sink.success(resp).await;
        };
        ctx.spawn(f);
    }

    fn report_split(
        &mut self,
        ctx: RpcContext,
        mut req: ReportSplitRequest,
        sink: UnarySink<ReportSplitResponse>,
    ) {
        let resp = check_bootstrap!(ctx, self.cluster, sink, req, ReportSplitResponse);
        self.cluster
            .put_regions(vec![req.take_left(), req.take_right()]);
        ctx.spawn(async move {
            let _ = sink.success(resp).await;
        });
    }

    fn ask_batch_split(
        &mut self,
        ctx: RpcContext,
        req: AskBatchSplitRequest,
        sink: UnarySink<AskBatchSplitResponse>,
    ) {
        let mut resp = check_bootstrap!(ctx, self.cluster, sink, req, AskBatchSplitResponse);
        let region = req.get_region();
        let split_count = req.get_split_count() as u64;
        let count = match self.get_split_id_count(region, split_count) {
            Ok(c) => c,
            Err(e) => {
                fill_error(resp.mut_header(), ErrorType::UNKNOWN, format!("{}", e));
                ctx.spawn(async move {
                    let _ = sink.success(resp).await;
                });
                return;
            }
        };
        let id = self.allocator.id().clone();
        let f = async move {
            match id.alloc(count).await {
                Ok(id) => {
                    let mut start_id = id - count + 1;
                    let peer_count = count / split_count as u64 - 1;
                    for _ in 0..split_count {
                        let mut id = SplitID::default();
                        id.set_new_region_id(start_id);
                        let new_peers = id.mut_new_peer_ids();
                        for i in 1..=peer_count {
                            new_peers.push(i + start_id);
                        }
                        start_id += peer_count + 1;
                        resp.mut_ids().push(id);
                    }
                }
                Err(e) => {
                    fill_error(resp.mut_header(), ErrorType::UNKNOWN, format!("{}", e));
                }
            }
            let _ = sink.success(resp).await;
        };
        ctx.spawn(f);
    }

    fn report_batch_split(
        &mut self,
        ctx: RpcContext,
        mut req: ReportBatchSplitRequest,
        sink: UnarySink<ReportBatchSplitResponse>,
    ) {
        let resp = check_bootstrap!(ctx, self.cluster, sink, req, ReportBatchSplitResponse);
        self.cluster.put_regions(req.take_regions().into());
        ctx.spawn(async move {
            let _ = sink.success(resp).await;
        });
    }

    fn get_cluster_config(
        &mut self,
        ctx: RpcContext,
        _req: GetClusterConfigRequest,
        sink: UnarySink<GetClusterConfigResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn put_cluster_config(
        &mut self,
        ctx: RpcContext,
        _req: PutClusterConfigRequest,
        sink: UnarySink<PutClusterConfigResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn scatter_region(
        &mut self,
        ctx: RpcContext,
        _req: ScatterRegionRequest,
        sink: UnarySink<ScatterRegionResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn get_gc_safe_point(
        &mut self,
        ctx: RpcContext,
        _req: GetGCSafePointRequest,
        sink: UnarySink<GetGCSafePointResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn update_gc_safe_point(
        &mut self,
        ctx: RpcContext,
        _req: UpdateGCSafePointRequest,
        sink: UnarySink<UpdateGCSafePointResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn update_service_gc_safe_point(
        &mut self,
        ctx: RpcContext,
        _req: UpdateServiceGCSafePointRequest,
        sink: UnarySink<UpdateServiceGCSafePointResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn sync_regions(
        &mut self,
        ctx: RpcContext,
        _stream: RequestStream<SyncRegionRequest>,
        sink: DuplexSink<SyncRegionResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn get_operator(
        &mut self,
        ctx: RpcContext,
        _req: GetOperatorRequest,
        sink: UnarySink<GetOperatorResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
    fn sync_max_ts(
        &mut self,
        ctx: RpcContext,
        _req: SyncMaxTSRequest,
        sink: UnarySink<SyncMaxTSResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }

    fn split_regions(
        &mut self,
        ctx: RpcContext,
        _req: SplitRegionsRequest,
        sink: UnarySink<SplitRegionsResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }

    fn get_dc_location_info(
        &mut self,
        ctx: RpcContext,
        _req: GetDCLocationInfoRequest,
        sink: UnarySink<GetDCLocationInfoResponse>,
    ) {
        grpcio::unimplemented_call!(ctx, sink)
    }
}
