use ::actix::prelude::*;
use futures::Future;
use redis_async::resp::RespValue;

use std::collections::HashMap;

use crate::command::*;
use crate::redis::RespValueWrapper;
use crate::Error;
use crate::RedisActor;

const MAX_RETRY: usize = 16;

fn fmt_resp_value(o: &::redis_async::resp::RespValue) -> String {
    match o {
        RespValue::Nil => "nil".to_string(),
        RespValue::Array(ref o) => format!(
            "[{}]",
            o.iter().map(fmt_resp_value).collect::<Vec<_>>().join(" ")
        ),
        RespValue::BulkString(ref o) => format!("\"{}\"", String::from_utf8_lossy(o)),
        RespValue::Error(ref o) => o.to_string(),
        RespValue::Integer(ref o) => o.to_string(),
        RespValue::SimpleString(ref o) => o.to_string(),
    }
}

pub struct RedisClusterActor {
    initial_addr: String,
    slots: Vec<Slots>,
    connections: HashMap<String, Addr<RedisActor>>,
}

impl RedisClusterActor {
    pub fn start<S: Into<String>>(addr: S) -> Addr<RedisClusterActor> {
        let addr = addr.into();

        Supervisor::start(move |_ctx| RedisClusterActor {
            initial_addr: addr,
            slots: vec![],
            connections: HashMap::new(),
        })
    }

    fn refresh_slots(&mut self) -> ResponseActFuture<Self, (), ()> {
        let addr = self.initial_addr.clone();
        let control_connection = self
            .connections
            .entry(addr.clone())
            .or_insert_with(move || RedisActor::start(addr));

        Box::new(
            control_connection
                .send(ClusterSlots)
                .then(|res| match res {
                    Ok(Ok(slots)) => Ok(slots),
                    Ok(Err(e)) => Err(e),
                    Err(_) => Err(Error::Disconnected),
                })
                .into_actor(self)
                .then(|res, this, _ctx| match res {
                    Ok(slots) => {
                        for slots in slots.iter() {
                            this.connections
                                .entry(slots.master().to_string())
                                .or_insert_with(|| {
                                    RedisActor::start(slots.master().clone())
                                });
                        }
                        this.slots = slots;
                        debug!("slots: {:?}", this.slots);
                        actix::fut::ok(())
                    }
                    Err(e) => {
                        warn!("refreshing slots failed: {:?}", e);
                        actix::fut::err(())
                    }
                }),
        )
    }
}

impl Actor for RedisClusterActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // TODO: does this wait prevent the issue (#1?)?
        ctx.wait(self.refresh_slots());
    }
}

impl Supervised for RedisClusterActor {
    fn restarting(&mut self, _: &mut Self::Context) {
        self.slots.clear();
        self.connections.clear();
    }
}

#[derive(Debug, Clone)]
struct Retry {
    addr: String,
    req: RespValue,
    retry: usize,
}

impl Message for Retry {
    type Result = Result<RespValue, Error>;
}

impl Retry {
    fn new(addr: String, req: RespValue, retry: usize) -> Self {
        Retry { addr, req, retry }
    }
}

impl Handler<Retry> for RedisClusterActor {
    type Result = ResponseActFuture<RedisClusterActor, RespValue, Error>;

    fn handle(&mut self, msg: Retry, _ctx: &mut Self::Context) -> Self::Result {
        fn do_retry(
            this: &mut RedisClusterActor,
            addr: String,
            req: RespValue,
            retry: usize,
        ) -> ResponseActFuture<RedisClusterActor, RespValue, Error> {
            use actix::fut::{err, ok};

            debug!(
                "processing: req = {}, addr = {}, retry = {}",
                fmt_resp_value(&req),
                addr,
                retry
            );

            let connection = this
                .connections
                .entry(addr.clone())
                .or_insert_with(move || RedisActor::start(addr));
            Box::new(
                connection
                    .send(RespValueWrapper(req.clone()))
                    .into_actor(this)
                    .then(move |res, this, ctx| {
                        debug!(
                            "received: {:?}",
                            res.as_ref().map(|res| res.as_ref().map(fmt_resp_value))
                        );
                        match res {
                            Ok(Ok(RespValue::Error(ref e)))
                                if e.starts_with("MOVED") && retry < MAX_RETRY =>
                            {
                                info!(
                                    "MOVED redirection: retry = {}, request = {}",
                                    retry,
                                    fmt_resp_value(&req)
                                );

                                let mut values = e.split(' ');
                                let _moved = values.next().unwrap();
                                let _slot = values.next().unwrap();
                                let addr = values.next().unwrap();

                                ctx.wait(this.refresh_slots());

                                do_retry(this, addr.to_string(), req, retry + 1)
                            }
                            Ok(Ok(RespValue::Error(ref e)))
                                if e.starts_with("ASK") && retry < MAX_RETRY =>
                            {
                                info!(
                                    "ASK redirection: retry = {}, request = {}",
                                    retry,
                                    fmt_resp_value(&req)
                                );

                                let mut values = e.split(' ');
                                let _moved = values.next().unwrap();
                                let _slot = values.next().unwrap();
                                let addr = values.next().unwrap();

                                ctx.spawn(
                                    // No retry for ASKING
                                    do_retry(
                                        this,
                                        addr.to_string(),
                                        Asking.into_request(),
                                        MAX_RETRY,
                                    )
                                    .then(
                                        |res, _this, _ctx| {
                                            match res.map(Asking::from_response) {
                                                Ok(Ok(())) => {}
                                                e => warn!(
                                                    "failed to issue ASKING: {:?}",
                                                    e
                                                ),
                                            };
                                            ok(())
                                        },
                                    ),
                                );

                                do_retry(this, addr.to_string(), req, retry + 1)
                            }
                            Ok(Ok(res)) => Box::new(ok(res)),
                            Ok(Err(e)) => Box::new(err(e)),
                            Err(_canceled) => Box::new(err(Error::Disconnected)),
                        }
                    }),
            )
        }

        do_retry(self, msg.addr, msg.req, msg.retry)
    }
}

impl<M> Handler<M> for RedisClusterActor
where
    M: Command
        + Message<Result = Result<<M as Command>::Output, Error>>
        + Send
        + 'static,
    <M as Command>::Output: Send + 'static,
{
    type Result = ResponseActFuture<RedisClusterActor, M::Output, Error>;

    fn handle(&mut self, msg: M, ctx: &mut Self::Context) -> Self::Result {
        use futures::IntoFuture;

        // refuse operations over multiple slots
        let slot = match msg.key_slot() {
            Ok(slot) => slot,
            Err(e) => return Box::new(actix::fut::err(Error::MultipleSlot(e))),
        };
        let req = msg.into_request();

        let fut = (|| match slot {
            Some(slot) => {
                for slots in self.slots.iter() {
                    if slots.start <= slot && slot <= slots.end {
                        return actix::Handler::handle(
                            self,
                            Retry::new(slots.master().to_string(), req, 0),
                            ctx,
                        );
                    }
                }

                warn!("no node is serving the slot {}", slot);
                Box::new(actix::fut::err(Error::NotConnected))
            }
            None => actix::Handler::handle(
                self,
                Retry::new(self.initial_addr.clone(), req, 0),
                ctx,
            ),
        })();

        Box::new(fut.and_then(|res, this, _ctx| {
            M::from_response(res)
                .map_err(Error::Redis)
                .into_future()
                .into_actor(this)
        }))
    }
}

#[doc(hidden)]
pub struct Stop;

impl Message for Stop {
    type Result = ();
}

impl Handler<Stop> for RedisClusterActor {
    type Result = ();

    fn handle(&mut self, _: Stop, ctx: &mut Self::Context) -> Self::Result {
        ctx.stop();
    }
}
