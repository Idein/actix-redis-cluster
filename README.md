# Actix redis cluster

Redis Cluster integration for actix framework.

## How does this repository differ from Actix redis?

This repository is a fork of [actix-redis](https://github.com/actix/actix-redis), which adds the support for Redis Cluster. Since the implementation is highly experimental and it contains breaking changes in the interface, we decided to publish this as a separate repository.

## Documentation

* [API Documentation (Development)](http://actix.github.io/actix-redis/actix_redis/)
* [API Documentation (Releases)](https://docs.rs/actix-redis/)
* [Chat on gitter](https://gitter.im/actix/actix)
* Cargo package: [actix-redis](https://crates.io/crates/actix-redis)
* Minimum supported Rust version: 1.26 or later


## Redis session backend

Use redis as session storage.

You need to pass an address of the redis server and random value to the
constructor of `RedisSessionBackend`. This is private key for cookie session,
When this value is changed, all session data is lost.

Note that whatever you write into your session is visible by the user (but not modifiable).

Constructor panics if key length is less than 32 bytes.

```rust
extern crate actix_web;
extern crate actix_redis;

use actix_web::{App, server, middleware};
use actix_web::middleware::session::SessionStorage;
use actix_redis::RedisSessionBackend;

fn main() {
    ::std::env::set_var("RUST_LOG", "actix_web=info");
    env_logger::init();
    let sys = actix::System::new("basic-example");

    server::new(
        || App::new()
            // enable logger
            .middleware(middleware::Logger::default())
            // cookie session middleware
            .middleware(SessionStorage::new(
                RedisSessionBackend::new("127.0.0.1:6379", &[0; 32])
            ))
            // register simple route, handle all methods
            .resource("/", |r| r.f(index)))
        .bind("0.0.0.0:8080").unwrap()
        .start();

    let _ = sys.run();
}
```

## License

This project is licensed under either of

* Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0))
* MIT license ([LICENSE-MIT](LICENSE-MIT) or [http://opensource.org/licenses/MIT](http://opensource.org/licenses/MIT))

at your option.

## Code of Conduct

Contribution to the actix-redis crate is organized under the terms of the
Contributor Covenant, the maintainer of actix-redis, @fafhrd91, promises to
intervene to uphold that code of conduct.
