use actix::prelude::*;
use actix_redis::{command::*, RedisClusterActor, RespValue};
use futures::Future;

#[test]
fn test_cluster_lua_eval_sha() {
    let _ = env_logger::try_init();
    let sys = System::new("test");

    let addr = RedisClusterActor::start("127.0.0.1:7000");

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
