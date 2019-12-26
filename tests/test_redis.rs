extern crate redis_async;

use actix_redis::{command::*, Error, RedisActor};

#[actix_rt::test]
async fn test_error_connect() {
    let addr = RedisActor::start("localhost:54000");
    let _addr2 = addr.clone();

    let res = addr.send(Get { key: "test".into() }).await;
    match res {
        Ok(Err(Error::NotConnected)) => (),
        _ => panic!("Should not happen {:?}", res),
    }
}

#[actix_rt::test]
async fn test_redis() {
    env_logger::init();

    let addr = RedisActor::start("127.0.0.1:6379");
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
                    println!("RESP: {:?}", resp);
                    assert_eq!(resp, b"value");
                }
                _ => panic!("Should not happen {:?}", res),
            }
        }
        _ => panic!("Should not happen {:?}", res),
    }
}
