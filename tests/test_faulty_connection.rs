use actix::prelude::*;
use actix_redis::{command::*, RedisActor};
use futures::future::{loop_fn, Loop};
use futures::Future;

// test whether RedisActor will eventually reconnects to Redis server
// TODO: we need to stop toxiproxy *after* a client connected. call REST API?
#[ignore]
#[test]
fn test_faulty_connection() -> std::io::Result<()> {
    env_logger::init();
    let sys = System::new("test-faulty-connection");

    // proxy server
    let addr = RedisActor::start("127.0.0.1:7379");
    let _addr2 = addr.clone();

    Arbiter::spawn_fn(move || {
        loop_fn(false, move |failed| {
            addr.send(Ping(None))
                .map_err(|e| panic!("Should not happen: {:?}", e))
                .map(move |res| {
                    let current = res.is_err();

                    match (failed, current) {
                        // failed to reconnect
                        (true, true) => Loop::Continue(true),
                        // succeeded to reconnect
                        (true, false) => Loop::Break(()),
                        // disconnected
                        (false, true) => Loop::Continue(true),
                        // not disconnected yet
                        (false, false) => Loop::Continue(false),
                    }
                })
        })
        .map(|()| System::current().stop())
    });

    sys.run()
}
