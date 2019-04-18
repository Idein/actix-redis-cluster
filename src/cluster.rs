use std::collections::HashMap;
use std::marker::PhantomData;

use ::actix::actors::resolver::{Connect, Resolver};
use ::actix::prelude::*;
use futures::Future;
use redis_async::resp::{RespCodec, RespValue};

use tokio_codec::Framed;
use tokio_tcp::TcpStream;

use crate::command::*;
use crate::slot::Hasher;
use crate::Error;

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

type ReadHalf = futures::stream::Wait<
    futures::stream::SplitStream<
        tokio_codec::Framed<tokio_tcp::TcpStream, redis_async::resp::RespCodec>,
    >,
>;
type WriteHalf = futures::sink::Wait<
    futures::stream::SplitSink<
        tokio_codec::Framed<tokio_tcp::TcpStream, redis_async::resp::RespCodec>,
    >,
>;

struct Node {
    read: ReadHalf,
    write: WriteHalf,
}

impl Node {
    fn new(stream: TcpStream) -> Self {
        use futures::{Sink, Stream};

        let framed = Framed::new(stream, RespCodec);
        let (sink, stream) = framed.split();

        Node {
            read: stream.wait(),
            write: sink.wait(),
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
    pub fn start<S: IntoIterator<Item = String>>(
        threads: usize,
        addrs: S,
    ) -> Addr<RedisClusterActor> {
        let addrs: Vec<_> = addrs.into_iter().collect();

        SyncArbiter::start(threads, move || RedisClusterActor {
            addrs: addrs.clone(),
            nodes: HashMap::new(),
            stale_slots: true,
            slots: vec![],
        })
    }
}

impl Actor for RedisClusterActor {
    type Context = SyncContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let addrs = self.addrs.clone();
        for addr in addrs.into_iter() {
            if let Err(_) = self.connect_to_node(addr) {
                ctx.stop();
            };
        }

        match self.random_node_addr() {
            Some(addr) => {
                if let Err(_) = self.retrieve_cluster_slots(addr.clone(), ctx) {
                    ctx.stop()
                }
            }
            None => ctx.stop(),
        }
    }
}

impl Supervised for RedisClusterActor {
    fn restarting(&mut self, _: &mut Self::Context) {
        self.nodes.clear();
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

    fn connect_to_node(&mut self, addr: String) -> Result<(), ()> {
        if self.nodes.contains_key(&addr) {
            return Ok(());
        }

        let stream = Resolver::from_registry()
            .send(Connect::host(&addr))
            .wait()
            .map_err(|e| warn!("Failed to send a message to the resolver: {:?}", e))
            .and_then(|x| x.map_err(|e| warn!("Failed to resolve the host: {:?}", e)))?;

        info!("Connected to redis server: {}", addr);

        // sanity check
        assert!(self.nodes.insert(addr, Node::new(stream)).is_none());

        Ok(())
    }

    fn retrieve_cluster_slots(
        &mut self,
        addr: String,
        ctx: &mut SyncContext<Self>,
    ) -> Result<(), Error> {
        let slots = self.handle(
            RawRequest::<ClusterSlots>::new(addr, ClusterSlots.into_request(), 0),
            ctx,
        );

        match slots {
            Ok(slots) => {
                self.stale_slots = false;
                self.slots = slots
                    .into_iter()
                    .filter_map(|(start, end, addrs)| {
                        if !addrs.is_empty() {
                            let master_addr = addrs.into_iter().next().unwrap();
                            self.connect_to_node(master_addr.clone()).unwrap(); // TODO: handle error

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
                debug!("{:?}", self.slots);
                Ok(())
            }
            Err(e) => {
                warn!("cannot retrieve CLUSTER SLOTS: {:?}", e);
                Err(e)
            }
        }
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
    type Result = Result<M::Output, Error>;

    fn handle(&mut self, msg: RawRequest<M>, ctx: &mut Self::Context) -> Self::Result {
        let RawRequest {
            addr, req, retry, ..
        } = msg;

        match self.nodes.get_mut(&addr) {
            Some(node) => {
                node.write.send(req.clone())?;
                node.write.flush()?;

                match node.read.next() {
                    Some(res) => match res {
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

                            self.stale_slots = true;
                            self.connect_to_node(addr.to_string()).map_err(|()| {
                                error!("Could not connect to redirected node: {}", addr);
                                Error::NotConnected
                            })?;

                            self.handle(
                                RawRequest::<M>::new(addr.to_string(), req, retry + 1),
                                ctx,
                            )
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

                            self.connect_to_node(addr.to_string()).map_err(|()| {
                                error!("Could not connect to redirected node: {}", addr);
                                Error::NotConnected
                            })?;

                            // first, send ASKING command
                            self.handle(
                                RawRequest::<Asking>::new(
                                    addr.to_string(),
                                    Asking.into_request(),
                                    0,
                                ),
                                ctx,
                            )?;

                            self.handle(
                                RawRequest::<M>::new(addr.to_string(), req, retry),
                                ctx,
                            )
                        }
                        Ok(res) => match M::from_response(res) {
                            Ok(v) => Ok(v),
                            Err(e) => Err(Error::Redis(e)),
                        },
                        Err(e) => Err(Error::Redis(e)),
                    },
                    None => Err(Error::Disconnected),
                }
            }
            None => Err(Error::NotConnected),
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
    type Result = Result<M::Output, Error>;

    fn handle(&mut self, msg: M, ctx: &mut Self::Context) -> Self::Result {
        // refuse operations over multiple slots
        let mut hasher = Hasher::new();
        msg.hash_keys(&mut hasher)?;
        let slot = hasher.get();
        let req = msg.into_request();
        let addr = self.choose_node_addr(slot).cloned();

        match addr {
            None => {
                warn!("RedisClusterActor is not connected to any nodes");
                Err(Error::NotConnected)
            }
            Some(addr) => self.handle(RawRequest::<M>::new(addr, req, 0), ctx),
        }
    }
}
