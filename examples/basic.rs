#![allow(unused_variables)]
#![cfg_attr(feature = "cargo-clippy", allow(needless_pass_by_value))]

extern crate actix;
extern crate actix_redis;
extern crate actix_web;
extern crate env_logger;
extern crate futures;

use actix_redis::RedisSessionBackend;
use actix_web::middleware::session::{self, RequestSession};
use actix_web::{middleware, server, App, HttpRequest, HttpResponse, Result};

/// simple handler
fn index(req: &HttpRequest) -> Result<HttpResponse> {
    println!("{:?}", req);

    // session
    if let Some(count) = req.session().get::<i32>("counter")? {
        println!("SESSION value: {}", count);
        req.session().set("counter", count + 1)?;
    } else {
        req.session().set("counter", 1)?;
    }

    Ok("Welcome!".into())
}

fn main() {
    ::std::env::set_var("RUST_LOG", "actix_web=info,actix_redis=info");
    env_logger::init();
    let sys = actix::System::new("basic-example");

    server::new(|| {
        App::new()
            // enable logger
            .middleware(middleware::Logger::default())
            // cookie session middleware
            .middleware(session::SessionStorage::new(RedisSessionBackend::new(
                "127.0.0.1:6379",
                &[0; 32],
            )))
            // register simple route, handle all methods
            .resource("/", |r| r.f(index))
    })
    .bind("0.0.0.0:8080")
    .unwrap()
    .workers(1)
    .start();

    let _ = sys.run();
}
