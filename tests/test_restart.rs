use actix_redis::{cluster::Stop, command::*, RedisClusterActor};
use futures::future::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};

#[actix_rt::test]
async fn test_cluster() {
    env_logger::init();

    let addr = RedisClusterActor::start("127.0.0.1:7000");

    // set value
    let set: FuturesUnordered<_> = (0..10)
        .map(|i| {
            addr.send(Set {
                key: format!("test{}", i),
                value: format!("value{}", i),
                expiration: Expiration::Infinite,
            })
        })
        .collect();

    set.collect::<Vec<_>>().await;

    // stop actor
    addr.send(Stop).await.unwrap();

    // see whether the actor restart and handles messages successfully
    let get: FuturesUnordered<_> = (0..10)
        .map(|i| {
            addr.send(Get {
                key: format!("test{}", i),
            })
            .map(move |v| {
                assert_eq!(
                    v.unwrap().unwrap().unwrap(),
                    format!("value{}", i).as_bytes()
                )
            })
        })
        .collect();

    get.collect::<Vec<_>>().await;
}
