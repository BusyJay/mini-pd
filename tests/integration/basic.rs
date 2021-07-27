use crate::cluster::Cluster;
use futures::channel::mpsc;
use futures::StreamExt;
use mini_pd::*;

#[futures_test::test]
async fn test_single_node() {
    let mut cluster = Cluster::new(1, 1);
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

    sender.send(Msg::snapshot(tx.clone())).unwrap();
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
    let mut cluster = Cluster::new(5, 3);
    cluster.start();

    let mut sender = cluster.server(1).sender();
    let (tx, mut rx) = mpsc::channel(10);

    sender
        .send(Msg::WaitEvent {
            event: Event::Elected,
            notifier: tx.clone(),
        })
        .unwrap();
    let leader = match rx.next().await {
        Some(Res::RoleInfo { leader, .. }) => leader,
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
        sender.send(Msg::snapshot(tx.clone())).unwrap();
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
