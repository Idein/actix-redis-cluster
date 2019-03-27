use actix::Message;
use redis_async::resp::RespValue;
use crate::slot::hash_slot;
use crate::Error;
use crate::RespError;

pub trait Command {
    type Output;

    /// Convert this command into a raw representation
    fn into_request(self) -> RespValue;

    /// Parse the response of the command
    fn from_response(res: RespValue) -> Result<Self::Output, RespError>;

    /// Calculate the slot number of the keys of this command.
    /// If all keys are stored in the same slot, Ok(Some(`slot`)) is returned.
    /// If there is no key in the command, Ok(None) is returned.
    ///
    /// # Failures
    /// If the keys are stored in different slots, this function reports an error with those slot
    /// numbers.
    fn key_slot(&self) -> Result<Option<u16>, Vec<u16>>;
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

    fn key_slot(&self) -> Result<Option<u16>, Vec<u16>> {
        Ok(Some(hash_slot(self.key.as_bytes())))
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

    fn key_slot(&self) -> Result<Option<u16>, Vec<u16>> {
        Ok(Some(hash_slot(self.key.as_bytes())))
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

    fn key_slot(&self) -> Result<Option<u16>, Vec<u16>> {
        Ok(Some(hash_slot(self.key.as_bytes())))
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

    fn key_slot(&self) -> Result<Option<u16>, Vec<u16>> {
        // calculate hash slot for each key and accumulate them if there are different slots
        self.keys.iter().fold(Ok(None), |accum, key| {
            let slot = hash_slot(key.as_bytes());
            match accum {
                Ok(None) => Ok(Some(slot)),
                Ok(Some(s)) if s == slot => Ok(Some(slot)),
                Ok(Some(s)) => Err(vec![s, slot]),
                Err(mut s) => {
                    s.push(slot);
                    Err(s)
                }
            }
        })
    }
}

#[derive(Debug)]
pub struct ClusterSlots;

impl Message for ClusterSlots {
    type Result = Result<Vec<(u16, u16, Vec<String>)>, Error>;
}

impl Command for ClusterSlots {
    type Output = Vec<(u16, u16, Vec<String>)>;

    fn into_request(self) -> RespValue {
        resp_array!["CLUSTER", "SLOTS"]
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RespError> {
        use redis_async::resp::FromResp;

        fn parse_entry(res: RespValue) -> Result<(u16, u16, Vec<String>), RespError> {
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
                                        let port = usize::from_resp(it.next().unwrap())?;

                                        nodes.push(format!("{}:{}", addr, port));
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

                        Ok((start, end, nodes))
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

    fn key_slot(&self) -> Result<Option<u16>, Vec<u16>> {
        Ok(None)
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

    fn key_slot(&self) -> Result<Option<u16>, Vec<u16>> {
        Ok(None)
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

    fn key_slot(&self) -> Result<Option<u16>, Vec<u16>> {
        Ok(Some(hash_slot(self.key.as_bytes())))
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

    fn key_slot(&self) -> Result<Option<u16>, Vec<u16>> {
        Ok(Some(hash_slot(self.key.as_bytes())))
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

    fn key_slot(&self) -> Result<Option<u16>, Vec<u16>> {
        Ok(Some(hash_slot(self.key.as_bytes())))
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

    fn key_slot(&self) -> Result<Option<u16>, Vec<u16>> {
        Ok(Some(hash_slot(self.key.as_bytes())))
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

    fn key_slot(&self) -> Result<Option<u16>, Vec<u16>> {
        Ok(Some(hash_slot(self.key.as_bytes())))
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

    fn key_slot(&self) -> Result<Option<u16>, Vec<u16>> {
        Ok(Some(hash_slot(self.key.as_bytes())))
    }
}

#[derive(Debug)]
pub struct Ping(Option<String>);

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

    fn key_slot(&self) -> Result<Option<u16>, Vec<u16>> {
        Ok(None)
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

    fn key_slot(&self) -> Result<Option<u16>, Vec<u16>> {
        Ok(None)
    }
}
