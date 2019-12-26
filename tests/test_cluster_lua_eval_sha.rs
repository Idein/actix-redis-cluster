use actix_redis::{command::*, RedisClusterActor, RespValue};

#[actix_rt::test]
async fn test_cluster_lua_eval_sha() {
    env_logger::init();

    let addr = RedisClusterActor::start("127.0.0.1:7000");

    let res = addr
        .send(ScriptLoad {
            script: r#"
            redis.call('SET', KEYS[1], ARGV[1])
            return tonumber(redis.call('GET', KEYS[2]))
            "#,
            slot: 15206, //slot of "actix"
        })
        .await;

    match res {
        Ok(Ok(hash)) => {
            let res = addr
                .send(EvalSha {
                    hash,
                    keys: vec!["actix".into(), "actix".into()],
                    args: vec!["21".into()],
                })
                .await;

            match res {
                Ok(Ok(RespValue::Integer(21))) => (),
                _ => panic!("Should not happen {:?}", res),
            }
        }
        _ => panic!("Should not happen {:?}", res),
    }
}
