use std::{sync::Arc, time::Duration};

use grpcio::{ChannelBuilder, Environment};
use kvproto::{pdpb::IsBootstrappedRequest, pdpb_grpc::PdClient};

use crate::cluster::Cluster;

#[futures_test::test]
async fn test_bootstrap() {
    let mut cluster = Cluster::new(5, 3);
    cluster.start();

    let addr = cluster.server(1).advertise_address();
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env.clone()).connect(addr);
    channel.wait_for_connected(Duration::from_secs(10)).await;
    let client = PdClient::new(channel);
    let resp = client
        .is_bootstrapped_async(&IsBootstrappedRequest::default())
        .unwrap()
        .await
        .unwrap();
    assert!(resp.get_header().has_error(), "{:?}", resp);
}
