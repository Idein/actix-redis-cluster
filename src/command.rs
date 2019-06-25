use crate::slot::{HashError, Hasher};
use crate::Error;
use crate::RespError;
use actix::Message;
use redis_async::resp::RespValue;

pub trait Command {
    type Output;

    /// Convert this command into a raw representation
    fn into_request(self) -> RespValue;

    /// Parse the response of the command
    fn from_response(res: RespValue) -> Result<Self::Output, RespError>;

    /// Calculate the slot number of the keys of this command.
    /// If all keys (including zero) fall into the same slot, Ok(()) is returned.
    ///
    /// # Failures
    /// If the keys falls into different slots, an error is reported
    fn hash_keys(&self, hasher: &mut Hasher) -> Result<(), HashError>;

    /// Calculate the slot number of the keys of this command.
    /// If there is no keys, return Ok(None)
    ///
    /// # Failures
    /// If the keys falls into different slots, an error is reported
    fn key_slot(&self) -> Result<Option<u16>, HashError> {
        let mut hasher = Hasher::new();
        self.hash_keys(&mut hasher)?;
        Ok(hasher.get())
    }
}

#[derive(Debug)]
pub struct Get {
    pub key: String,
}

impl Message for Get {
    type Result = Result<Option<Vec<u8>>, Error>;
}

impl Command for Get {
    type Output = Option<Vec<u8>>;

    fn into_request(self) -> RespValue {
        resp_array!["GET", self.key]
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        match res {
            RespValue::BulkString(s) => Ok(Some(s)),
            RespValue::Nil => Ok(None),
            _ => Err(RespError::RESP(
                "invalid response for GET".into(),
                Some(res),
            )),
        }
    }

    fn hash_keys(&self, hasher: &mut Hasher) -> Result<(), HashError> {
        hasher.hash_str(&self.key)
    }
}

#[derive(Debug)]
pub enum Expiration {
    Infinite,
    Ex(String),
    Px(String),
}

#[derive(Debug)]
pub struct Set {
    pub key: String,
    pub value: String,
    pub expiration: Expiration,
}

impl Message for Set {
    type Result = Result<(), Error>;
}

impl Command for Set {
    type Output = ();

    fn into_request(self) -> RespValue {
        use self::Expiration::*;

        match self.expiration {
            Infinite => resp_array!["SET", self.key, self.value],
            Ex(ex) => resp_array!["SET", self.key, self.value, "EX", ex],
            Px(px) => resp_array!["SET", self.key, self.value, "PX", px],
        }
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        // TODO: SET with NX/XX can return Null reply
        match res {
            RespValue::SimpleString(ref s) if s == "OK" => Ok(()),
            _ => Err(RespError::RESP(
                "invalid response for SET".into(),
                Some(res),
            )),
        }
    }

    fn hash_keys(&self, hasher: &mut Hasher) -> Result<(), HashError> {
        hasher.hash_str(&self.key)
    }
}

#[derive(Debug)]
pub struct Expire {
    pub key: String,
    pub seconds: String,
}

impl Message for Expire {
    type Result = Result<bool, Error>;
}

impl Command for Expire {
    /// true if the timeout was set, false if key does not exist
    type Output = bool;

    fn into_request(self) -> RespValue {
        resp_array!["EXPIRE", self.key, self.seconds]
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        match res {
            RespValue::Integer(1) => Ok(true),
            RespValue::Integer(0) => Ok(false),
            _ => Err(RespError::RESP(
                "invalid response for EXPIRE".into(),
                Some(res),
            )),
        }
    }

    fn hash_keys(&self, hasher: &mut Hasher) -> Result<(), HashError> {
        hasher.hash_str(&self.key)
    }
}

#[derive(Debug)]
pub struct Del {
    pub keys: Vec<String>,
}

impl Message for Del {
    type Result = Result<i64, Error>;
}

impl Command for Del {
    /// the number of keys that were removed
    type Output = i64;

    fn into_request(self) -> RespValue {
        let mut v = vec![RespValue::BulkString(b"DEL".to_vec())];
        v.extend(self.keys.into_iter().map(Into::into));
        RespValue::Array(v)
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        match res {
            RespValue::Integer(num) => Ok(num),
            _ => Err(RespError::RESP(
                "invalid response for DEL".into(),
                Some(res),
            )),
        }
    }

    fn hash_keys(&self, hasher: &mut Hasher) -> Result<(), HashError> {
        for key in self.keys.iter() {
            hasher.hash_str(key)?
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ClusterSlots;

#[derive(Clone, Debug)]
pub struct Slots {
    pub start: u16,
    pub end: u16,
    /// IP address, port, id of nodes serving the slots.
    /// The first entry corresponds to the master node.
    pub nodes: Vec<(String, i64, Option<String>)>,
}

impl Slots {
    pub fn master(&self) -> String {
        format!("{}:{}", self.nodes[0].0, self.nodes[0].1)
    }
}

impl Message for ClusterSlots {
    type Result = Result<Vec<Slots>, Error>;
}

impl Command for ClusterSlots {
    type Output = Vec<Slots>;

    fn into_request(self) -> RespValue {
        resp_array!["CLUSTER", "SLOTS"]
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        use redis_async::resp::FromResp;

        fn parse_entry(res: RespValue) -> Result<Slots, RespError> {
            match res {
                RespValue::Array(values) => {
                    if values.len() >= 3 {
                        let mut it = values.into_iter();
                        let start = u32::from_resp(it.next().unwrap())? as u16;
                        let end = u32::from_resp(it.next().unwrap())? as u16;

                        let mut nodes = vec![];
                        for node in it {
                            match node {
                                RespValue::Array(node) => {
                                    if node.len() >= 2 {
                                        let mut it = node.into_iter();
                                        let addr =
                                            String::from_resp(it.next().unwrap())?;
                                        let port = i64::from_resp(it.next().unwrap())?;
                                        let id = it
                                            .next()
                                            .and_then(|x| String::from_resp(x).ok());

                                        nodes.push((addr, port, id));
                                    } else {
                                        return Err(RespError::RESP(
                                            "invalid response for CLUSTER SLOTS".into(),
                                            Some(RespValue::Array(node)),
                                        ));
                                    }
                                }
                                _ => {
                                    return Err(RespError::RESP(
                                        "invalid response for CLUSTER SLOTS".into(),
                                        Some(node),
                                    ));
                                }
                            }
                        }

                        Ok(Slots { start, end, nodes })
                    } else {
                        Err(RespError::RESP(
                            "invalid response for CLUSTER SLOTS".into(),
                            Some(RespValue::Array(values)),
                        ))
                    }
                }
                _ => Err(RespError::RESP(
                    "invalid response for CLUSTER SLOTS".into(),
                    Some(res),
                )),
            }
        }

        match res {
            RespValue::Array(inner) => inner
                .into_iter()
                .map(parse_entry)
                .collect::<Result<Vec<_>, _>>(),
            _ => Err(RespError::RESP(
                "invalid response for CLUSTER SLOTS".into(),
                Some(res),
            )),
        }
    }

    fn hash_keys(&self, _hasher: &mut Hasher) -> Result<(), HashError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct Asking;

impl Message for Asking {
    type Result = Result<(), Error>;
}

impl Command for Asking {
    type Output = ();

    fn into_request(self) -> RespValue {
        resp_array!["ASKING"]
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        match res {
            RespValue::SimpleString(ref s) if s == "OK" => Ok(()),
            res => Err(RespError::RESP(
                "invalid response for ASKING".into(),
                Some(res),
            )),
        }
    }

    fn hash_keys(&self, _hasher: &mut Hasher) -> Result<(), HashError> {
        Ok(())
    }
}

#[derive(Debug)]
pub enum TtlError {
    KeyNotExist,
    NoExpire,
    Unknown(i64),
}

impl std::fmt::Display for TtlError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use self::TtlError::*;
        match self {
            KeyNotExist => write!(f, "key does not exist"),
            NoExpire => write!(f, "key has no associated expire"),
            Unknown(x) => write!(f, "unknown error: {}", x),
        }
    }
}

impl std::error::Error for TtlError {}

#[derive(Debug)]
pub struct Ttl {
    pub key: String,
}

impl Message for Ttl {
    type Result = Result<Result<i64, TtlError>, Error>;
}

impl Command for Ttl {
    type Output = Result<i64, TtlError>;

    fn into_request(self) -> RespValue {
        resp_array!["TTL", self.key]
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        match res {
            RespValue::Integer(-2) => Ok(Err(TtlError::NoExpire)),
            RespValue::Integer(-1) => Ok(Err(TtlError::NoExpire)),
            RespValue::Integer(x) if x < 0 => Ok(Err(TtlError::Unknown(x))),
            RespValue::Integer(x) => Ok(Ok(x)),
            res => Err(RespError::RESP(
                "invalid response for TTL".into(),
                Some(res),
            )),
        }
    }

    fn hash_keys(&self, hasher: &mut Hasher) -> Result<(), HashError> {
        hasher.hash_str(&self.key)
    }
}

#[derive(Debug)]
pub struct Pttl {
    pub key: String,
}

impl Message for Pttl {
    type Result = Result<Result<i64, TtlError>, Error>;
}

impl Command for Pttl {
    type Output = Result<i64, TtlError>;

    fn into_request(self) -> RespValue {
        resp_array!["TTL", self.key]
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        match res {
            RespValue::Integer(-2) => Ok(Err(TtlError::NoExpire)),
            RespValue::Integer(-1) => Ok(Err(TtlError::NoExpire)),
            RespValue::Integer(x) if x < 0 => Ok(Err(TtlError::Unknown(x))),
            RespValue::Integer(x) => Ok(Ok(x)),
            res => Err(RespError::RESP(
                "invalid response for TTL".into(),
                Some(res),
            )),
        }
    }

    fn hash_keys(&self, hasher: &mut Hasher) -> Result<(), HashError> {
        hasher.hash_str(&self.key)
    }
}

#[derive(Debug)]
pub struct Incr {
    pub key: String,
}

impl Message for Incr {
    type Result = Result<Result<i64, String>, Error>;
}

impl Command for Incr {
    type Output = Result<i64, String>;

    fn into_request(self) -> RespValue {
        resp_array!["INCR", self.key]
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        match res {
            RespValue::Integer(x) => Ok(Ok(x)),
            RespValue::Error(e) => Ok(Err(e)),
            res => Err(RespError::RESP(
                "invalid response for INCR".into(),
                Some(res),
            )),
        }
    }

    fn hash_keys(&self, hasher: &mut Hasher) -> Result<(), HashError> {
        hasher.hash_str(&self.key)
    }
}

#[derive(Debug)]
pub struct IncrBy {
    pub key: String,
    pub increment: i64,
}

impl Message for IncrBy {
    type Result = Result<Result<i64, String>, Error>;
}

impl Command for IncrBy {
    type Output = Result<i64, String>;

    fn into_request(self) -> RespValue {
        resp_array!["INCRBY", self.key, RespValue::Integer(self.increment)]
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        match res {
            RespValue::Integer(x) => Ok(Ok(x)),
            RespValue::Error(e) => Ok(Err(e)),
            res => Err(RespError::RESP(
                "invalid response for INCRBY".into(),
                Some(res),
            )),
        }
    }

    fn hash_keys(&self, hasher: &mut Hasher) -> Result<(), HashError> {
        hasher.hash_str(&self.key)
    }
}

#[derive(Debug)]
pub struct Decr {
    pub key: String,
}

impl Message for Decr {
    type Result = Result<Result<i64, String>, Error>;
}

impl Command for Decr {
    type Output = Result<i64, String>;

    fn into_request(self) -> RespValue {
        resp_array!["DECR", self.key]
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        match res {
            RespValue::Integer(x) => Ok(Ok(x)),
            RespValue::Error(e) => Ok(Err(e)),
            res => Err(RespError::RESP(
                "invalid response for DECR".into(),
                Some(res),
            )),
        }
    }

    fn hash_keys(&self, hasher: &mut Hasher) -> Result<(), HashError> {
        hasher.hash_str(&self.key)
    }
}

#[derive(Debug)]
pub struct DecrBy {
    pub key: String,
    pub decrement: i64,
}

impl Message for DecrBy {
    type Result = Result<Result<i64, String>, Error>;
}

impl Command for DecrBy {
    type Output = Result<i64, String>;

    fn into_request(self) -> RespValue {
        resp_array!["DECRBY", self.key, RespValue::Integer(self.decrement)]
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        match res {
            RespValue::Integer(x) => Ok(Ok(x)),
            RespValue::Error(e) => Ok(Err(e)),
            res => Err(RespError::RESP(
                "invalid response for DECRBY".into(),
                Some(res),
            )),
        }
    }

    fn hash_keys(&self, hasher: &mut Hasher) -> Result<(), HashError> {
        hasher.hash_str(&self.key)
    }
}

#[derive(Debug)]
pub struct Ping(pub Option<String>);

impl Message for Ping {
    type Result = Result<String, Error>;
}

impl Command for Ping {
    type Output = String;

    fn into_request(self) -> RespValue {
        match self.0 {
            Some(s) => resp_array!["PING", s],
            None => resp_array!["PING"],
        }
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        match res {
            RespValue::SimpleString(s) => Ok(s),
            res => Err(RespError::RESP(
                "invalid response for PING".into(),
                Some(res),
            )),
        }
    }

    fn hash_keys(&self, _hasher: &mut Hasher) -> Result<(), HashError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct Echo(String);

impl Message for Echo {
    type Result = Result<String, Error>;
}

impl Command for Echo {
    type Output = String;

    fn into_request(self) -> RespValue {
        resp_array!["ECHO", self.0]
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        match res {
            RespValue::SimpleString(s) => Ok(s),
            res => Err(RespError::RESP(
                "invalid response for ECHO".into(),
                Some(res),
            )),
        }
    }

    fn hash_keys(&self, _hasher: &mut Hasher) -> Result<(), HashError> {
        Ok(())
    }
}

pub struct ScriptExists {
    pub hash: Vec<Vec<u8>>,
    pub slot: u16,
}

impl Message for ScriptExists {
    type Result = Result<Vec<bool>, Error>;
}

impl Command for ScriptExists {
    type Output = Vec<bool>;

    fn into_request(self) -> RespValue {
        let mut req = vec!["SCRIPT".into(), "EXISTS".into()];
        let mut args = self.hash.into_iter().map(RespValue::BulkString).collect();
        req.append(&mut args);
        RespValue::Array(req)
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        if let RespValue::Array(values) = &res {
            let bool_values = values
                .iter()
                .map(|v| match v {
                    RespValue::Integer(0) => Some(false),
                    RespValue::Integer(1) => Some(true),
                    _ => None,
                })
                .collect();
            if let Some(results) = bool_values {
                return Ok(results);
            }
        }
        Err(RespError::RESP(
            "invalid response for SCRIPT EXISTS".into(),
            Some(res),
        ))
    }

    fn hash_keys(&self, hasher: &mut Hasher) -> Result<(), HashError> {
        hasher.set(self.slot)
    }
}

pub struct ScriptLoad<'a> {
    pub script: &'a str,
    pub slot: u16,
}

impl<'a> Message for ScriptLoad<'a> {
    type Result = Result<Vec<u8>, Error>;
}

impl<'a> Command for ScriptLoad<'a> {
    type Output = Vec<u8>;

    fn into_request(self) -> RespValue {
        resp_array!["SCRIPT", "LOAD", self.script]
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        match res {
            RespValue::BulkString(hash) => Ok(hash),
            res => Err(RespError::RESP(
                "invalid response for SCRIPT LOAD".into(),
                Some(res),
            )),
        }
    }

    fn hash_keys(&self, hasher: &mut Hasher) -> Result<(), HashError> {
        hasher.set(self.slot)
    }
}

pub struct ScriptFlush {
    pub slot: u16,
}

impl Message for ScriptFlush {
    type Result = Result<String, Error>;
}

impl Command for ScriptFlush {
    type Output = String;

    fn into_request(self) -> RespValue {
        resp_array!["SCRIPT", "FLUSH"]
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        match res {
            RespValue::SimpleString(str) => Ok(str),
            res => Err(RespError::RESP(
                "invalid response for SCRIPT FLUSH".into(),
                Some(res),
            )),
        }
    }

    fn hash_keys(&self, hasher: &mut Hasher) -> Result<(), HashError> {
        hasher.set(self.slot)
    }
}

pub struct Eval<'a> {
    pub script: &'a str,
    pub keys: Vec<String>,
    pub args: Vec<RespValue>,
}

impl<'a> Message for Eval<'a> {
    type Result = Result<RespValue, Error>;
}

impl<'a> Command for Eval<'a> {
    type Output = RespValue;

    fn into_request(mut self) -> RespValue {
        let mut v = vec![
            "EVAL".into(),
            self.script.into(),
            format!("{}", self.keys.len()).into(),
        ];
        for key in self.keys {
            v.push(key.into());
        }
        v.append(&mut self.args);
        RespValue::Array(v)
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        Ok(res)
    }

    fn hash_keys(&self, hasher: &mut Hasher) -> Result<(), HashError> {
        for key in self.keys.iter() {
            hasher.hash_str(key)?
        }
        Ok(())
    }
}

pub struct EvalSha {
    pub hash: Vec<u8>,
    pub keys: Vec<String>,
    pub args: Vec<RespValue>,
}

impl Message for EvalSha {
    type Result = Result<RespValue, Error>;
}

impl Command for EvalSha {
    type Output = RespValue;

    fn into_request(mut self) -> RespValue {
        let mut v = vec![
            "EVALSHA".into(),
            RespValue::BulkString(self.hash),
            format!("{}", self.keys.len()).into(),
        ];
        for key in self.keys {
            v.push(key.into());
        }
        v.append(&mut self.args);
        RespValue::Array(v)
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        Ok(res)
    }

    fn hash_keys(&self, hasher: &mut Hasher) -> Result<(), HashError> {
        for key in self.keys.iter() {
            hasher.hash_str(key)?
        }
        Ok(())
    }
}

pub struct ClusterAddSlots {
    pub slots: Vec<u16>,
    pub target_node_slot: u16,
}

impl Message for ClusterAddSlots {
    type Result = Result<(), Error>;
}

impl Command for ClusterAddSlots {
    type Output = ();

    fn into_request(self) -> RespValue {
        let mut v = vec!["CLUSTER".into(), "ADDSLOTS".into()];
        v.extend(self.slots.into_iter().map(|x| x.to_string().into()));
        RespValue::Array(v)
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        match res {
            RespValue::SimpleString(ref s) if s == "OK" => Ok(()),
            _ => Err(RespError::RESP(
                "invalid response for CLUSTER ADDSLOTS".into(),
                Some(res),
            )),
        }
    }

    fn hash_keys(&self, hasher: &mut Hasher) -> Result<(), HashError> {
        hasher.set(self.target_node_slot)
    }
}

pub struct ClusterDelSlots {
    pub slots: Vec<u16>,
    pub target_node_slot: u16,
}

impl Message for ClusterDelSlots {
    type Result = Result<(), Error>;
}

impl Command for ClusterDelSlots {
    type Output = ();

    fn into_request(self) -> RespValue {
        let mut v = vec!["CLUSTER".into(), "DELSLOTS".into()];
        v.extend(self.slots.into_iter().map(|x| x.to_string().into()));
        RespValue::Array(v)
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        match res {
            RespValue::SimpleString(ref s) if s == "OK" => Ok(()),
            _ => Err(RespError::RESP(
                "invalid response for CLUSTER DELSLOTS".into(),
                Some(res),
            )),
        }
    }

    fn hash_keys(&self, hasher: &mut Hasher) -> Result<(), HashError> {
        hasher.set(self.target_node_slot)
    }
}

pub enum ClusterSetSlot {
    Migrating {
        slot: u16,
        destination_id: String,
        target_node_slot: u16,
    },
    Importing {
        slot: u16,
        source_id: String,
        target_node_slot: u16,
    },
    Stable {
        slot: u16,
        target_node_slot: u16,
    },
    Node {
        slot: u16,
        node_id: String,
        target_node_slot: u16,
    },
}

impl Message for ClusterSetSlot {
    type Result = Result<(), Error>;
}

impl Command for ClusterSetSlot {
    type Output = ();

    fn into_request(self) -> RespValue {
        use ClusterSetSlot::*;

        match self {
            Migrating {
                slot,
                destination_id,
                ..
            } => resp_array![
                "CLUSTER",
                "SETSLOT",
                slot.to_string(),
                "MIGRATING",
                destination_id
            ],
            Importing {
                slot, source_id, ..
            } => resp_array![
                "CLUSTER",
                "SETSLOT",
                slot.to_string(),
                "IMPORTING",
                source_id
            ],
            Stable { slot, .. } => {
                resp_array!["CLUSTER", "SETSLOT", slot.to_string(), "STABLE"]
            }
            Node { slot, node_id, .. } => {
                resp_array!["CLUSTER", "SETSLOT", slot.to_string(), "NODE", node_id]
            }
        }
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        match res {
            RespValue::SimpleString(ref s) if s == "OK" => Ok(()),
            _ => Err(RespError::RESP(
                "invalid response for CLUSTER SETSLOT".into(),
                Some(res),
            )),
        }
    }

    fn hash_keys(&self, hasher: &mut Hasher) -> Result<(), HashError> {
        use ClusterSetSlot::*;

        match self {
            Migrating {
                target_node_slot, ..
            }
            | Importing {
                target_node_slot, ..
            }
            | Stable {
                target_node_slot, ..
            }
            | Node {
                target_node_slot, ..
            } => hasher.set(*target_node_slot),
        }
    }
}

pub struct ClusterCountKeysInSlot {
    pub slot: u16,
    pub target_node_slot: u16,
}

impl Message for ClusterCountKeysInSlot {
    type Result = Result<usize, Error>;
}

impl Command for ClusterCountKeysInSlot {
    type Output = usize;

    fn into_request(self) -> RespValue {
        resp_array!["CLUSTER", "COUNTKEYSINSLOT", self.slot.to_string()]
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        match res {
            RespValue::Integer(v) if v >= 0 => Ok(v as usize),
            _ => Err(RespError::RESP(
                "invalid response for CLUSTER COUNTKEYSINSLOT".into(),
                Some(res),
            )),
        }
    }

    fn hash_keys(&self, hasher: &mut Hasher) -> Result<(), HashError> {
        hasher.set(self.target_node_slot)
    }
}

pub struct ClusterGetKeysInSlot {
    pub slot: u16,
    pub count: usize,
    pub target_node_slot: u16,
}

impl Message for ClusterGetKeysInSlot {
    type Result = Result<Vec<String>, Error>;
}

impl Command for ClusterGetKeysInSlot {
    type Output = Vec<String>;

    fn into_request(self) -> RespValue {
        resp_array![
            "CLUSTER",
            "GETKEYSINSLOT",
            self.slot.to_string(),
            self.count.to_string()
        ]
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        use redis_async::resp::FromResp;

        match res {
            RespValue::Array(v) => v.into_iter().map(String::from_resp).collect(),
            _ => Err(RespError::RESP(
                "invalid response for CLUSTER GETKEYSINSLOT".into(),
                Some(res),
            )),
        }
    }

    fn hash_keys(&self, hasher: &mut Hasher) -> Result<(), HashError> {
        hasher.set(self.target_node_slot)
    }
}

pub struct Migrate {
    pub host: String,
    pub port: usize,
    pub key: String,
    pub db: usize,
    pub timeout: usize,
    pub target_node_slot: u16,
}

impl Message for Migrate {
    type Result = Result<bool, Error>;
}

impl Command for Migrate {
    type Output = bool;

    fn into_request(self) -> RespValue {
        resp_array![
            "MIGRATE",
            self.host,
            self.port.to_string(),
            self.key,
            self.db.to_string(),
            self.timeout.to_string()
        ]
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        match res {
            RespValue::SimpleString(ref s) if s == "OK" => Ok(true),
            RespValue::SimpleString(ref s) if s == "NOKEY" => Ok(false),
            _ => Err(RespError::RESP(
                "invalid response for MIGRATE".into(),
                Some(res),
            )),
        }
    }

    fn hash_keys(&self, hasher: &mut Hasher) -> Result<(), HashError> {
        hasher.set(self.target_node_slot)
    }
}
