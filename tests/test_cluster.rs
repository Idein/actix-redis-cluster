use actix_redis::{command::*, RedisClusterActor};

#[actix_rt::test]
async fn test_cluster() {
    env_logger::init();

    let addr = RedisClusterActor::start("127.0.0.1:7000");

    let res = addr
        .send(Set {
            key: "test".into(),
            value: "value".into(),
            expiration: Expiration::Infinite,
        })
        .await;

    match res {
        Ok(Ok(())) => {
            let res = addr.send(Get { key: "test".into() }).await;
            match res {
                Ok(Ok(Some(resp))) => {
                    assert_eq!(resp, b"value");
                }
                _ => panic!("Should not happen {:?}", res),
            }
        }
        _ => panic!("Should not happen {:?}", res),
    }
}
