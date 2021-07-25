use std::{sync::Arc, time::Duration};

use futures::{channel::mpsc, join, SinkExt, StreamExt};
use futures_timer::Delay;
use grpcio::{ChannelBuilder, Environment, WriteFlags};
use kvproto::{pdpb::TsoRequest, pdpb_grpc::PdClient};
use mini_pd::{Msg, Res};

use crate::cluster::Cluster;

#[futures_test::test]
async fn test_tso() {
    let mut cluster = Cluster::new(5, 3);
    cluster.start();

    let sender = cluster.server(1).sender();
    let mut addr = cluster.server(1).advertise_address();
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env.clone()).connect(addr);
    channel.wait_for_connected(Duration::from_secs(10)).await;
    let client = PdClient::new(channel);
    let (mut tx, mut rx) = client.tso().unwrap();
    tx.send((TsoRequest::default(), WriteFlags::default()))
        .await
        .unwrap();
    let resp = rx.next().await.unwrap().unwrap();
    assert!(resp.get_header().has_error(), "{:?}", resp);

    let (tx, mut rx) = mpsc::channel(10);
    sender
        .send(Msg::WaitTillElected {
            leader: false,
            commit_to_current_term: false,
            notifier: tx.clone(),
        })
        .unwrap();
    let leader = match rx.next().await {
        Some(Res::RoleInfo { leader, .. }) => leader,
        res => panic!("failed to wait for election finish: {:?}", res),
    };

    Delay::new(Duration::from_millis(10)).await;
    addr = cluster.server(leader).advertise_address();
    let channel = ChannelBuilder::new(env.clone()).connect(addr);
    channel.wait_for_connected(Duration::from_secs(10)).await;
    let client = PdClient::new(channel);
    let (mut tx, mut rx) = client.tso().unwrap();
    let batch = &[3, 1, 5, 1, 4];
    let req = async move {
        for b in batch {
            let mut req = TsoRequest::default();
            req.set_count(*b);
            tx.send((req, WriteFlags::default().buffer_hint(true)))
                .await
                .unwrap();
        }
        tx.close().await.unwrap();
    };
    let resp = async move {
        let mut last_ts = 0;
        for b in batch {
            let resp = rx.next().await.unwrap().unwrap();
            assert_eq!(resp.get_count(), *b, "{:?}", resp);
            assert_ne!(resp.get_timestamp().get_logical(), last_ts, "{:?}", resp);
            last_ts = resp.get_timestamp().get_logical();
        }
        let res = rx.next().await;
        assert!(res.is_none(), "{:?}", res);
    };
    join!(req, resp);
}
