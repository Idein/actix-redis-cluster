use std::collections::{HashMap, VecDeque};
use std::io;
use std::marker::PhantomData;

use actix::actors::resolver::{Connect, Resolver};
use actix::io::FramedWrite;
use actix::prelude::*;
use futures::unsync::oneshot;
use futures::Future;
use redis_async::error::Error as RespError;
use redis_async::resp::{RespCodec, RespValue};
use tokio_codec::FramedRead;
use tokio_io::io::WriteHalf;
use tokio_io::AsyncRead;
use tokio_tcp::TcpStream;

use command::*;
use Error;

const MAX_RETRY: usize = 16;

fn fmt_resp_value(o: &::redis_async::resp::RespValue) -> String {
    use redis_async::resp::RespValue;
    match o {
        RespValue::Nil => format!("nil"),
        RespValue::Array(ref o) => format!(
            "[{}]",
            o.iter().map(fmt_resp_value).collect::<Vec<_>>().join(" ")
        ),
        RespValue::BulkString(ref o) => format!("\"{}\"", String::from_utf8_lossy(o)),
        RespValue::Error(ref o) => format!("{}", o),
        RespValue::Integer(ref o) => format!("{}", o),
        RespValue::SimpleString(ref o) => format!("{}", o),
    }
}

struct Node {
    connection: FramedWrite<WriteHalf<TcpStream>, RespCodec>,
    queue: VecDeque<oneshot::Sender<Result<RespValue, Error>>>,
}

impl Node {
    fn new(connection: FramedWrite<WriteHalf<TcpStream>, RespCodec>) -> Self {
        Node {
            connection,
            queue: VecDeque::new(),
        }
    }
}

#[derive(Debug)]
struct Slots {
    start: u16,
    end: u16,
    master_addr: String,
}

pub struct RedisClusterActor {
    addrs: Vec<String>,
    nodes: HashMap<String, Node>,
    stale_slots: bool,
    slots: Vec<Slots>,
}

impl RedisClusterActor {
    pub fn start<S: IntoIterator<Item = String>>(addrs: S) -> Addr<RedisClusterActor> {
        let addrs = addrs.into_iter().collect();

        Supervisor::start(|_| RedisClusterActor {
            addrs,
            nodes: HashMap::new(),
            stale_slots: true,
            slots: vec![],
        })
    }
}

impl Actor for RedisClusterActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let addrs = self.addrs.clone();
        for addr in addrs.into_iter() {
            self.connect_to_node(addr, ctx);
        }
    }
}

impl Supervised for RedisClusterActor {
    fn restarting(&mut self, _: &mut Self::Context) {
        self.nodes.clear();
    }
}

impl actix::io::WriteHandler<io::Error> for RedisClusterActor {
    fn error(&mut self, err: io::Error, _: &mut Self::Context) -> Running {
        warn!("Redis connection dropped: {}", err);
        Running::Stop
    }
}

impl StreamHandler<(String, RespValue), (String, RespError)> for RedisClusterActor {
    fn error(
        &mut self,
        (node_name, err): (String, RespError),
        _: &mut Self::Context,
    ) -> Running {
        debug!("handling error {:?} from {}", err, node_name);
        match self.nodes.get_mut(&node_name) {
            Some(node) => {
                match node.queue.pop_front() {
                    Some(tx) => {
                        let _ = tx.send(Err(err.into()));
                    }
                    // something wrong
                    None => warn!(
                        "there is no corresponding tx for {:?} in {}",
                        err, node_name,
                    ),
                }
            }
            // disconnected
            None => warn!(
                "already disconnected to {}. discarding: {:?}",
                node_name, err,
            ),
        }

        Running::Stop
    }

    fn handle(&mut self, (node_name, res): (String, RespValue), _: &mut Self::Context) {
        debug!("handling {} from {}", fmt_resp_value(&res), node_name);
        match self.nodes.get_mut(&node_name) {
            Some(node) => {
                match node.queue.pop_front() {
                    Some(tx) => {
                        let _ = tx.send(Ok(res));
                    }
                    // something wrong
                    None => warn!(
                        "there is no corresponding tx for {:?} in {}",
                        res, node_name,
                    ),
                }
            }
            // disconnected
            None => warn!(
                "already disconnected to {}. discarding: {}",
                node_name,
                fmt_resp_value(&res),
            ),
        }
    }
}

impl RedisClusterActor {
    fn random_node_addr(&self) -> Option<&String> {
        let mut rng = rand::thread_rng();
        match rand::seq::sample_iter(&mut rng, self.nodes.keys(), 1) {
            Ok(v) => {
                let addr = v.into_iter().next().unwrap();
                debug!("random node: {}", addr);
                Some(addr)
            }
            Err(_) => None,
        }
    }

    fn choose_node_addr(&self, slot: Option<u16>) -> Option<&String> {
        if let Some(slot) = slot {
            for slots in self.slots.iter() {
                if slots.start <= slot && slot <= slots.end {
                    return Some(&slots.master_addr);
                }
            }
        }

        self.random_node_addr()
    }

    fn connect_to_node(&mut self, addr: String, ctx: &mut Context<Self>) {
        use futures::Stream;
        use std::collections::hash_map::Entry;

        if self.nodes.contains_key(&addr) {
            return;
        }

        let addr2 = addr.clone();

        let fut = Resolver::from_registry()
            .send(Connect::host(&addr))
            .into_actor(self)
            .map(move |response, act, ctx| {
                let mut connected = false;

                match response {
                    Ok(stream) => match act.nodes.entry(addr2.clone()) {
                        // If the actor is not connected to the node yet, subscribe the stream
                        Entry::Vacant(e) => {
                            info!("Connected to redis server: {}", e.key());
                            connected = true;

                            let (r, w) = stream.split();
                            let mut framed = FramedWrite::new(w, RespCodec, ctx);
                            e.insert(Node::new(framed));

                            let addr3 = addr2.clone();
                            ctx.add_stream(FramedRead::new(r, RespCodec).then(
                                move |res| match res {
                                    Ok(res) => Ok((addr3.clone(), res)),
                                    Err(e) => Err((addr3.clone(), e)),
                                },
                            ));
                        }
                        // If the actor happen to connect the same node twice, ignore the latter
                        // because updating node info will lose all commands in the queue
                        _ => {}
                    },
                    Err(e) => {
                        // TODO: reconnect
                        error!(
                            "Can not connect to redis server: {}, address: {}",
                            e, addr2
                        );
                    }
                }

                if connected && act.stale_slots {
                    act.retrieve_cluster_slots(addr2, ctx);
                }
            })
            .map_err(|err, _act, _ctx| error!("Can not access Resolver: {}", err));

        // wait for establishing a connection
        ctx.wait(fut);
    }

    fn retrieve_cluster_slots(&mut self, addr: String, ctx: &mut Context<Self>) {
        let actor_addr = ctx.address();
        let fut = actor_addr
            .send(RawRequest::<ClusterSlots>::new(
                addr,
                ClusterSlots.into_request(),
                0,
            ))
            .map_err(|_err| {})
            .into_actor(self)
            .map(|slots, act, ctx| match slots {
                Ok(slots) => {
                    act.stale_slots = false;
                    act.slots = slots
                        .into_iter()
                        .filter_map(|(start, end, addrs)| {
                            if !addrs.is_empty() {
                                let master_addr = addrs.into_iter().next().unwrap();
                                act.connect_to_node(master_addr.clone(), ctx);

                                Some(Slots {
                                    start,
                                    end,
                                    master_addr,
                                })
                            } else {
                                None
                            }
                        })
                        .collect();
                    debug!("{:?}", act.slots);
                }
                Err(e) => {
                    warn!("cannot retrieve CLUSTER SLOTS: {:?}", e);
                }
            });
        ctx.spawn(fut);
    }
}

struct RawRequest<M> {
    addr: String,
    req: RespValue,
    retry: usize,
    phantom: PhantomData<M>,
}

impl<M> Message for RawRequest<M>
where
    M: Command
        + Message<Result = Result<<M as Command>::Output, Error>>
        + Send
        + 'static,
    <M as Command>::Output: Send + 'static,
{
    type Result = M::Result;
}

impl<M> RawRequest<M> {
    fn new(addr: String, req: RespValue, retry: usize) -> Self {
        RawRequest {
            addr,
            req,
            retry,
            phantom: PhantomData,
        }
    }
}

impl<M> Handler<RawRequest<M>> for RedisClusterActor
where
    M: Command
        + Message<Result = Result<<M as Command>::Output, Error>>
        + Send
        + 'static,
    <M as Command>::Output: Send + 'static,
{
    type Result = ResponseActFuture<Self, M::Output, Error>;

    fn handle(&mut self, msg: RawRequest<M>, _: &mut Self::Context) -> Self::Result {
        use futures::future::{err, ok};

        let RawRequest {
            addr, req, retry, ..
        } = msg;

        // NLL will eliminate the necessity of this enum
        enum Operation {
            Enqueue(oneshot::Receiver<Result<RespValue, Error>>),
            Exit,
        }

        // as we can write `.into_actor(self)` directly in this match
        let status = match self.nodes.get_mut(&addr) {
            Some(node) => {
                let (tx, rx) = oneshot::channel();
                node.queue.push_back(tx);
                node.connection.write(req.clone());
                Operation::Enqueue(rx)
            }
            None => Operation::Exit,
        };

        match status {
            Operation::Enqueue(rx) => Box::new({
                rx.map_err(|_| Error::Disconnected)
                    .into_actor(self)
                    .and_then(move |res, actor, ctx| match res {
                        // handle MOVED redirection
                        Ok(RespValue::Error(ref e))
                            if e.starts_with("MOVED") && retry < MAX_RETRY =>
                        {
                            info!(
                                "MOVED redirection: retry = {}, request = {}",
                                retry,
                                fmt_resp_value(&req)
                            );

                            let mut values = e.split(' ');
                            let _moved = values.next().unwrap();
                            // TODO: add to slot table
                            let _slot = values.next().unwrap();
                            let addr = values.next().unwrap();

                            actor.stale_slots = true;
                            actor.connect_to_node(addr.to_string(), ctx);

                            let actor_addr = ctx.address();
                            Box::new(
                                actor_addr
                                    .send(RawRequest::<M>::new(
                                        addr.to_string(),
                                        req,
                                        retry + 1,
                                    ))
                                    .map_err(|_| Error::Disconnected)
                                    .and_then(|res| res)
                                    .into_actor(actor),
                            ) as Self::Result
                        }
                        Ok(RespValue::Error(ref e)) if e.starts_with("ASK") => {
                            info!(
                                "MOVED redirection: retry = {}, request = {}",
                                retry,
                                fmt_resp_value(&req)
                            );

                            let mut values = e.split(' ');
                            let _ask = values.next().unwrap();
                            let _slot = values.next().unwrap();
                            let addr = values.next().unwrap();

                            actor.connect_to_node(addr.to_string(), ctx);

                            let actor_addr = ctx.address();

                            // first, send ASKING command
                            ctx.spawn(
                                actor_addr
                                    .send(RawRequest::<Asking>::new(
                                        addr.to_string(),
                                        Asking.into_request(),
                                        retry,
                                    ))
                                    .map_err(|_| ())
                                    .map(|res| {
                                        if let Err(e) = res {
                                            error!("ASKING returned an error: {}", e);
                                        }
                                    })
                                    .into_actor(actor),
                            );

                            // then, send redirected command
                            Box::new(
                                actor_addr
                                    .send(RawRequest::<M>::new(
                                        addr.to_string(),
                                        req,
                                        retry,
                                    ))
                                    .map_err(|_| Error::Disconnected)
                                    .and_then(|res| res)
                                    .into_actor(actor),
                            ) as Self::Result
                        }
                        Ok(res) => Box::new(
                            match M::from_response(res) {
                                Ok(v) => ok(v),
                                Err(e) => err(Error::Redis(e)),
                            }
                            .into_actor(actor),
                        ),
                        Err(e) => Box::new(err(e).into_actor(actor)),
                    })
            }),
            Operation::Exit => Box::new(err(Error::NotConnected).into_actor(self)),
        }
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
    type Result = ResponseFuture<M::Output, Error>;

    fn handle(&mut self, msg: M, ctx: &mut Self::Context) -> Self::Result {
        use futures::future::err;

        // refuse operations over multiple slots
        let slot = match msg.key_slot() {
            Ok(slot) => slot,
            Err(e) => return Box::new(err(Error::MultipleSlot(e))),
        };
        let req = msg.into_request();
        let addr = self.choose_node_addr(slot).cloned();
        let actor_addr = ctx.address();

        // TODO: choose node by hash of keys
        match addr {
            None => {
                warn!("RedisClusterActor is not connected to any nodes");
                Box::new(err(Error::NotConnected))
            }
            Some(addr) => Box::new(
                actor_addr
                    .send(RawRequest::<M>::new(addr, req, 0))
                    .map_err(|_| Error::Disconnected)
                    .and_then(|res| res),
            ),
        }
    }
}
