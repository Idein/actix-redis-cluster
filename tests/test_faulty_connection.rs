use actix_redis::{command::*, RedisActor};
use futures::TryFutureExt;

// test whether RedisActor will eventually reconnects to Redis server
// TODO: we need to stop toxiproxy *after* a client connected. call REST API?
#[actix_rt::test]
#[ignore]
async fn test_faulty_connection() {
    env_logger::init();

    // proxy server
    let addr = RedisActor::start("127.0.0.1:7379");
    let mut last = true;

    loop {
        let res = addr
            .send(Ping(None))
            .map_err(|e| panic!("Should not happen: {:?}", e))
            .await;

        let current = res.is_ok();

        if !last && current {
            break;
        }

        last = current;
    }
}
