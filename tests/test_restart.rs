use actix::prelude::*;
use actix_redis::{cluster::Stop, command::*, RedisClusterActor};
use futures::Future;

#[test]
fn test_cluster() -> std::io::Result<()> {
    env_logger::init();
    let sys = System::new("test-cluster-restart");

    let addr = RedisClusterActor::start("127.0.0.1:7000");

    Arbiter::spawn_fn(move || {
        // set value
        let set = futures::future::join_all((0..10).map({
            let addr = addr.clone();
            move |i| {
                addr.send(Set {
                    key: format!("test{}", i),
                    value: format!("value{}", i),
                    expiration: Expiration::Infinite,
                })
            }
        }));

        // stop actor
        let stop = set.and_then({
            let addr = addr.clone();
            move |_| addr.send(Stop)
        });

        // see whether the actor restart and handles messages successfully
        let get = stop.and_then(move |()| {
            futures::future::join_all((0..10).map({
                let addr = addr.clone();
                move |i| {
                    addr.send(Get {
                        key: format!("test{}", i),
                    })
                    .map(move |v| {
                        assert_eq!(v.unwrap().unwrap(), format!("value{}", i).as_bytes())
                    })
                }
            }))
        });

        get.map(|_| {
            System::current().stop();
        })
        .map_err(|e| panic!("Should not happen {:?}", e))
    });

    sys.run()
}
