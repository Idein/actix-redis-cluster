//! Redis integration for Actix framework.
//!
//! ## Documentation
//! * [API Documentation (Development)](http://actix.github.io/actix-redis/actix_redis/)
//! * [API Documentation (Releases)](https://docs.rs/actix-redis/)
//! * [Chat on gitter](https://gitter.im/actix/actix)
//! * Cargo package: [actix-redis](https://crates.io/crates/actix-redis)
//! * Minimum supported Rust version: 1.26 or later
//!
#[macro_use]
extern crate log;
#[macro_use]
extern crate redis_async;
#[macro_use]
extern crate derive_more;

pub mod cluster;
pub mod command;
pub mod redis;
pub mod slot;
pub use crate::cluster::RedisClusterActor;
pub use crate::redis::RedisActor;

#[cfg(feature = "web")]
mod session;
#[cfg(feature = "web")]
pub use actix_web::cookie::SameSite;
#[cfg(feature = "web")]
pub use session::RedisSession;

/// General purpose actix redis error
#[derive(Debug, Display, From)]
pub enum Error {
    #[display(fmt = "Redis error {}", _0)]
    Redis(redis_async::error::Error),
    /// Receiving message during reconnecting
    #[display(fmt = "Redis: Not connected")]
    NotConnected,
    /// Cancel all waters when connection get dropped
    #[display(fmt = "Redis: Disconnected")]
    Disconnected,
    /// Trying to access multiple slots at once in cluster mode
    #[display(fmt = "Redis: Multiple slot command {:?}", _0)]
    MultipleSlot(slot::HashError),
    /// I/O Error
    #[display(fmt = "Redis: I/O error {}", _0)]
    IoError(std::io::Error),
}

#[cfg(feature = "web")]
impl actix_web::ResponseError for Error {}

// re-export
pub use redis_async::error::Error as RespError;
pub use redis_async::resp::RespValue;
