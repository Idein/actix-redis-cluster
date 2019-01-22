extern crate actix;
extern crate actix_redis;
extern crate env_logger;
extern crate futures;

use actix::prelude::*;
use actix_redis::{command::*, RedisClusterActor};
use futures::Future;

#[test]
fn test_cluster_discovery() {
    env_logger::init();
    let sys = System::new("test");

    let addr = RedisClusterActor::start(vec![
        "127.0.0.1:7000".into(),
        "127.0.0.1:7001".into(),
        "127.0.0.1:7002".into(),
    ]);

    Arbiter::spawn_fn(move || {
        let addr2 = addr.clone();
        addr.send(Set {
            key: "test".into(),
            value: "value".into(),
            expiration: Expiration::Infinite,
        })
        .then(move |res| match res {
            Ok(Ok(())) => addr2.send(Get { key: "test".into() }).then(|res| {
                match res {
                    Ok(Ok(Some(resp))) => {
                        assert_eq!(resp, b"value");
                    }
                    _ => panic!("Should not happen {:?}", res),
                }
                System::current().stop();
                Ok(())
            }),
            _ => panic!("Should not happen {:?}", res),
        })
    });

    sys.run();
}
