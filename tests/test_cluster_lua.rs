extern crate actix;
extern crate actix_redis;
extern crate env_logger;
extern crate futures;

use actix::prelude::*;
use actix_redis::{command::*, RedisClusterActor, RespValue};
use futures::Future;

#[test]
fn test_cluster_lua_eval() {
    let _ = env_logger::try_init();
    let sys = System::new("test");

    let addr = RedisClusterActor::start(vec![
        "127.0.0.1:7000".into(),
        "127.0.0.1:7001".into(),
        "127.0.0.1:7002".into(),
    ]);

    Arbiter::spawn_fn(move || {
        addr.send(Eval {
            script: r#"
                redis.call('SET', KEYS[1], ARGV[1])
                return tonumber(redis.call('GET', KEYS[2]))
                "#,
            keys: vec!["bar".into(), "bar".into()],
            args: vec!["21".into()],
        })
        .then(move |res| match res {
            Ok(Ok(RespValue::Integer(21))) => {
                System::current().stop();
                Ok(())
            }
            _ => panic!("Should not happen {:?}", res),
        })
    });

    sys.run();
}

#[test]
fn test_cluster_lua_load_script() {
    let _ = env_logger::try_init();
    let sys = System::new("test");

    let addr = RedisClusterActor::start(vec![
        "127.0.0.1:7000".into(),
        "127.0.0.1:7001".into(),
        "127.0.0.1:7002".into(),
    ]);

    Arbiter::spawn_fn(move || {
        addr.send(ScriptLoad {
            script: "return 1",
            slot: 0,
        })
        .then(move |res| match res {
            Ok(Ok(hash)) => {
                let fake_hash = b"0".to_vec();
                addr.send(ScriptExists {
                    hash: vec![hash, fake_hash],
                    slot: 0,
                })
                .then(move |res| match &res {
                    Ok(Ok(results)) => {
                        if *results == vec![true, false] {
                            System::current().stop();
                            Ok(())
                        } else {
                            panic!("Should not happen {:?}", res)
                        }
                    }
                    _ => panic!("Should not happen {:?}", res),
                })
            }
            _ => panic!("Should not happen {:?}", res),
        })
    });

    sys.run();
}

#[test]
fn test_cluster_lua_eval_sha() {
    let _ = env_logger::try_init();
    let sys = System::new("test");

    let addr = RedisClusterActor::start(vec![
        "127.0.0.1:7000".into(),
        "127.0.0.1:7001".into(),
        "127.0.0.1:7002".into(),
    ]);

    Arbiter::spawn_fn(move || {
        addr.send(ScriptLoad {
            script: r#"
                redis.call('SET', KEYS[1], ARGV[1])
                return tonumber(redis.call('GET', KEYS[2]))
                "#,
            slot: 15206, //slot of "actix"
        })
        .then(move |res| match res {
            Ok(Ok(hash)) => addr
                .send(EvalSha {
                    hash: hash,
                    keys: vec!["actix".into(), "actix".into()],
                    args: vec!["21".into()],
                })
                .then(move |res| match &res {
                    Ok(Ok(RespValue::Integer(21))) => {
                        System::current().stop();
                        Ok(())
                    }
                    _ => panic!("Should not happen {:?}", res),
                }),
            _ => panic!("Should not happen {:?}", res),
        })
    });

    sys.run();
}
