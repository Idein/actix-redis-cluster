use actix_redis::{command::*, RedisClusterActor, RespValue};

#[actix_rt::test]
async fn test_cluster_lua_eval() {
    env_logger::init();

    let addr = RedisClusterActor::start("127.0.0.1:7000");

    let res = addr
        .send(Eval {
            script: r#"
            redis.call('SET', KEYS[1], ARGV[1])
            return tonumber(redis.call('GET', KEYS[2]))
            "#,
            keys: vec!["bar".into(), "bar".into()],
            args: vec!["21".into()],
        })
        .await;

    match res {
        Ok(Ok(RespValue::Integer(21))) => (),
        _ => panic!("Should not happen {:?}", res),
    }
}
