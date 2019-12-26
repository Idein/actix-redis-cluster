use actix_redis::{command::*, RedisClusterActor};
use futures::stream::{FuturesUnordered, StreamExt};
use std::fmt::Debug;
use std::time::Duration;
use tokio::time::delay_for;

fn success<T: Debug, E1: Debug, E2: Debug>(res: Result<Result<T, E1>, E2>) -> T {
    match res {
        Ok(Ok(x)) => x,
        _ => panic!("Should not happen {:?}", res),
    }
}

#[actix_rt::test]
async fn test_cluster_redirect() {
    env_logger::init();

    let addr = RedisClusterActor::start("127.0.0.1:7000");

    let set = Set {
        key: "test-moved".into(),
        value: "value".into(),
        expiration: Expiration::Infinite,
    };
    let slot = set.key_slot().unwrap().unwrap();

    success(addr.send(set).await);
    let slots = success(addr.send(ClusterSlots).await);

    let mut source = None;
    let mut source_slot = None;
    let mut destination = None;
    let mut destination_slot = None;

    // find a slot where `test-moved` is stored and another slot (where `test-moved` is NOT stored)
    for slots in slots.into_iter() {
        if slots.start <= slot && slot <= slots.end {
            source = Some(slots.nodes[0].clone());
            source_slot = Some(slots.start);
        } else {
            destination = Some(slots.nodes[0].clone());
            destination_slot = Some(slots.start);
        }

        if source.is_some() && destination.is_some() {
            break;
        }
    }

    let (_source_host, _source_port, source_id) = source.unwrap();
    let source_id = source_id.unwrap();
    let (destination_host, destination_port, destination_id) = destination.unwrap();
    let destination_id = destination_id.unwrap();
    let source_slot = source_slot.unwrap();
    let destination_slot = destination_slot.unwrap();

    success(
        addr.send(ClusterSetSlot::Importing {
            slot,
            source_id: source_id.clone(),
            target_node_slot: destination_slot,
        })
        .await,
    );

    success(
        addr.send(ClusterSetSlot::Migrating {
            slot,
            destination_id: destination_id.clone(),
            target_node_slot: source_slot,
        })
        .await,
    );

    let mut count = success(
        addr.send(ClusterCountKeysInSlot {
            slot,
            target_node_slot: source_slot,
        })
        .await,
    );

    while count > 0 {
        let keys = success(
            addr.send(ClusterGetKeysInSlot {
                slot,
                count: 10,
                target_node_slot: source_slot,
            })
            .await,
        );
        let len = keys.len();

        let migration: FuturesUnordered<_> = keys
            .into_iter()
            .map({
                let addr = addr.clone();
                let host = destination_host.clone();
                let port = destination_port as usize;
                move |key| {
                    addr.send(Migrate {
                        host: host.clone(),
                        port,
                        key,
                        db: 0,
                        timeout: 100,
                        target_node_slot: source_slot,
                    })
                }
            })
            .collect();

        let res: Vec<_> = migration.collect().await;
        for res in res.into_iter() {
            assert!(success(res));
        }

        count -= len;
    }

    // test ASK redirection
    assert_eq!(
        success(
            addr.send(Get {
                key: "test-moved".into(),
            })
            .await
        )
        .unwrap(),
        b"value"
    );

    success(
        addr.send(ClusterSetSlot::Node {
            slot,
            node_id: destination_id,
            target_node_slot: destination_slot,
        })
        .await,
    );

    // wait until migration completes
    delay_for(Duration::from_secs(3)).await;

    // test MOVED redirection
    assert_eq!(
        success(
            addr.send(Get {
                key: "test-moved".into(),
            })
            .await
        )
        .unwrap(),
        b"value"
    );
}
