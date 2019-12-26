use actix_redis::{command::*, RedisClusterActor};

#[actix_rt::test]
async fn test_cluster_lua_load_script() {
    env_logger::init();

    let addr = RedisClusterActor::start("127.0.0.1:7000");

    let res = addr
        .send(ScriptLoad {
            script: "return 1",
            slot: 0,
        })
        .await;

    match res {
        Ok(Ok(hash)) => {
            let fake_hash = b"0".to_vec();
            let res = addr
                .send(ScriptExists {
                    hash: vec![hash, fake_hash],
                    slot: 0,
                })
                .await;

            match &res {
                Ok(Ok(results)) => {
                    if *results != vec![true, false] {
                        panic!("Should not happen {:?}", res)
                    }
                }
                _ => panic!("Should not happen {:?}", res),
            }
        }
        _ => panic!("Should not happen {:?}", res),
    }
}
