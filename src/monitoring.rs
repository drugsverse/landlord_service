use actix_web::{get, App, HttpResponse, HttpServer};

#[get("/health/live")]
async fn liveness() -> HttpResponse {
    HttpResponse::Ok().finish()
}

#[get("/health/ready")]
async fn readiness() -> HttpResponse {
    HttpResponse::Ok().finish()
}

#[get("/metrics")]
async fn metrics() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/plain; version=0.0.4")
        .body("# Landlord metrics goes here\n")
}

pub async fn run(host: String, port: u16) -> std::io::Result<()> {
    let bind_address = format!("{host}:{port}");

    // Create the factory first and bind it.
    let configured_server = HttpServer::new(|| {
        App::new()
            .service(liveness)
            .service(readiness)
            .service(metrics)
    })
    .workers(2)
    .bind(&bind_address)?;

    // Isolate the conversion of the server into its background runner handle.
    // This drops the non Send configured_server.
    let server_handle = {
        let runner = configured_server.run();
        runner // Return the clean actix_web::dev::Server wrapper
    };

    // Await the server handle. This is fully thread safe and Send
    server_handle.await
}
