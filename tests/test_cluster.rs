use actix::prelude::*;
use actix_redis::{command::*, RedisClusterActor};
use futures::Future;

#[test]
fn test_cluster() -> std::io::Result<()> {
    env_logger::init();
    let sys = System::new("test");

    let addr = RedisClusterActor::start("127.0.0.1:7000");

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

    sys.run()
}
