use crate::cluster::Cluster;
use futures::channel::mpsc;
use futures::StreamExt;
use mini_pd::*;
use sloggers::terminal::{Destination, TerminalLoggerBuilder};
use sloggers::types::Severity;
use sloggers::Build;

#[futures_test::test]
async fn test_single_node() {
    let mut builder = TerminalLoggerBuilder::new();
    builder.level(Severity::Debug);
    builder.destination(Destination::Stderr);
    let logger = builder.build().unwrap();

    let mut cluster = Cluster::new(1, 1, logger);
    cluster.start();

    let sender = cluster.server(1).sender();
    let put = Command::Put {
        key: "dk1".into(),
        value: "dv1".into(),
    };
    let (tx, mut rx) = mpsc::channel(10);
    sender.send(Msg::command(put, Some(tx.clone()))).unwrap();
    let res = rx.next().await;
    assert!(matches!(res, Some(Res::Success)), "{:?}", res);

    sender.send(Msg::Snapshot(tx.clone())).unwrap();
    let snap = rx.next().await;
    match snap {
        Some(Res::Snapshot(s)) => {
            let val = s.get(b"dk1").unwrap().unwrap();
            assert_eq!(b"dv1", &*val);
        }
        s => panic!("wrong result {:?}", s),
    }
}

#[futures_test::test]
async fn test_multi_node() {
    let mut builder = TerminalLoggerBuilder::new();
    builder.level(Severity::Debug);
    builder.destination(Destination::Stderr);
    let logger = builder.build().unwrap();

    let mut cluster = Cluster::new(5, 3, logger);
    cluster.start();

    let mut sender = cluster.server(1).sender();
    let (tx, mut rx) = mpsc::channel(10);

    sender.send(Msg::WaitTillLeader(tx.clone())).unwrap();
    let leader = match rx.next().await {
        Some(Res::Leader(id)) => id,
        res => panic!("failed to wait for election finish: {:?}", res),
    };

    sender = cluster.server(leader).sender();

    let put = Command::Put {
        key: "dk1".into(),
        value: "dv1".into(),
    };
    sender.send(Msg::command(put, Some(tx.clone()))).unwrap();
    let res = rx.next().await;
    assert!(matches!(res, Some(Res::Success)), "{:?}", res);

    for id in 1..=3 {
        let sender = cluster.server(id).sender();
        sender.send(Msg::Snapshot(tx.clone())).unwrap();
        let snap = rx.next().await.unwrap();
        match snap {
            Res::Snapshot(s) => {
                let val = s.get(b"dk1").unwrap().unwrap();
                assert_eq!(b"dv1", &*val);
            }
            s => panic!("wrong result {:?}", s),
        }
    }
}
