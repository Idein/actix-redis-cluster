use actix_redis::{command::*, RedisActor};
use actix_web::client::Client;
use futures::TryFutureExt;
use std::time::Duration;
use tokio::time::delay_for;

// test whether RedisActor will eventually reconnects to Redis server
#[actix_rt::test]
async fn test_faulty_connection() {
    const TOXIPROXY_ADDR: &'static str = "http://127.0.0.1:8474/proxies/redis";

    env_logger::init();

    let (sender, receiver) = futures::channel::oneshot::channel();

    actix::spawn(async move {
        // proxy server
        let addr = RedisActor::start("127.0.0.1:7379");
        let mut last = true;

        loop {
            let res = addr
                .send(Ping(None))
                .map_err(|e| panic!("Should not happen: {:?}", e))
                .await
                .unwrap();
            println!("res = {:?}", res);

            let current = res.is_ok();

            if !last && current {
                break;
            }

            last = current;
            tokio::task::yield_now().await;
        }

        sender.send(()).unwrap();
    });

    let client = Client::new();

    delay_for(Duration::from_secs(3)).await;
    client
        .post(TOXIPROXY_ADDR)
        .send_body(r#"{"enabled":false}"#)
        .await
        .unwrap();

    delay_for(Duration::from_secs(3)).await;
    client
        .post(TOXIPROXY_ADDR)
        .send_body(r#"{"enabled":true}"#)
        .await
        .unwrap();

    receiver.await.unwrap();
}
