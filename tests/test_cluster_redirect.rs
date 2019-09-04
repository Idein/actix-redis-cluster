use actix::prelude::*;
use actix_redis::{command::*, RedisClusterActor};
use futures::Future;
use tokio_timer::sleep;

use std::time::Duration;

#[test]
fn test_cluster_moved() -> std::io::Result<()> {
    env_logger::init();
    let sys = System::new("test-moved");

    let addr = RedisClusterActor::start("127.0.0.1:7000");

    Arbiter::spawn_fn(move || {
        let set = Set {
            key: "test-moved".into(),
            value: "value".into(),
            expiration: Expiration::Infinite,
        };
        let slot = set.key_slot().unwrap().unwrap();
        addr.send(set)
            .and_then({
                let addr = addr.clone();
                move |res| {
                    res.unwrap();

                    addr.send(ClusterSlots)
                }
            })
            .and_then({
                let addr = addr.clone();
                move |slots| {
                    let mut source = None;
                    let mut source_slot = None;
                    let mut destination = None;
                    let mut destination_slot = None;

                    for slots in slots.unwrap().into_iter() {
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
                    let (destination_host, destination_port, destination_id) =
                        destination.unwrap();
                    let destination_id = destination_id.unwrap();
                    let source_slot = source_slot.unwrap();
                    let destination_slot = destination_slot.unwrap();

                    addr.send(ClusterSetSlot::Importing {
                        slot,
                        source_id: source_id.clone(),
                        target_node_slot: destination_slot,
                    })
                    .and_then({
                        let addr = addr.clone();
                        let destination_id = destination_id.clone();
                        move |res| {
                            res.unwrap();

                            addr.send(ClusterSetSlot::Migrating {
                                slot,
                                destination_id,
                                target_node_slot: source_slot,
                            })
                        }
                    })
                    .and_then({
                        let addr = addr.clone();
                        move |res| {
                            res.unwrap();

                            addr.send(ClusterCountKeysInSlot {
                                slot,
                                target_node_slot: source_slot,
                            })
                        }
                    })
                    .and_then({
                        use futures::future::{join_all, loop_fn, Loop};

                        let addr = addr.clone();
                        move |res| {
                            let count = res.unwrap();

                            loop_fn(count, move |count| {
                                addr.send(ClusterGetKeysInSlot {
                                    slot,
                                    count: 10,
                                    target_node_slot: source_slot,
                                })
                                .and_then({
                                    let addr = addr.clone();
                                    let destination_host = destination_host.clone();
                                    move |keys| {
                                        let keys = keys.unwrap();
                                        let len = keys.len();

                                        join_all(keys.into_iter().map({
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
                                        }))
                                        .map(
                                            move |res| {
                                                for res in res.into_iter() {
                                                    res.unwrap();
                                                }

                                                if count <= len {
                                                    Loop::Break(())
                                                } else {
                                                    Loop::Continue(count - len)
                                                }
                                            },
                                        )
                                    }
                                })
                            })
                        }
                    })
                    // test ASK redirection
                    .and_then({
                        let addr = addr.clone();
                        move |()| {
                            addr.send(Get {
                                key: "test-moved".into(),
                            })
                            .map(|res| assert_eq!(res.unwrap().unwrap(), b"value"))
                        }
                    })
                    .and_then({
                        let addr = addr.clone();
                        move |()| {
                            addr.send(ClusterSetSlot::Node {
                                slot,
                                node_id: destination_id,
                                target_node_slot: destination_slot,
                            })
                        }
                    })
                    .and_then(move |res| {
                        res.unwrap();

                        // wait until migration completes
                        sleep(Duration::from_secs(3)).map_err(|_| MailboxError::Timeout)
                    })
                    // test MOVED redirection
                    .and_then({
                        let addr = addr.clone();
                        move |()| {
                            addr.send(Get {
                                key: "test-moved".into(),
                            })
                            .map(|res| assert_eq!(res.unwrap().unwrap(), b"value"))
                        }
                    })
                }
            })
            .map(|()| {
                System::current().stop();
            })
            .map_err(|e| panic!("Should not happen {:?}", e))
    });

    sys.run()
}
