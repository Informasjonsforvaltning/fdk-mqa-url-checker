use actix_web::{get, App, HttpServer, Responder};
use fdk_mqa_url_checker::{
    kafka::{
        create_sr_settings, run_async_processor, BROKERS, INPUT_TOPIC, OUTPUT_TOPIC,
        SCHEMA_REGISTRY,
    },
    metrics::{get_metrics, register_metrics},
    schemas::setup_schemas,
};
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};

#[get("/ping")]
async fn ping() -> impl Responder {
    "pong"
}

#[get("/ready")]
async fn ready() -> impl Responder {
    "ok"
}

#[get("/metrics")]
async fn metrics() -> impl Responder {
    match get_metrics() {
        Ok(metrics) => metrics,
        Err(e) => {
            tracing::error!(error = e.to_string(), "unable to gather metrics");
            "".to_string()
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .with_current_span(false)
        .init();

    register_metrics();

    tracing::info!(
        brokers = BROKERS.to_string(),
        schema_registry = SCHEMA_REGISTRY.to_string(),
        input_topic = INPUT_TOPIC.to_string(),
        output_topic = OUTPUT_TOPIC.to_string(),
        "starting service"
    );

    let sr_settings = create_sr_settings().unwrap_or_else(|e| {
        tracing::error!(error = e.to_string(), "sr settings creation error");
        std::process::exit(1);
    });

    setup_schemas(&sr_settings).await.unwrap_or_else(|e| {
        tracing::error!(error = e.to_string(), "schema registration error");
        std::process::exit(1);
    });

    let http_server = tokio::spawn(
        HttpServer::new(|| App::new().service(ping).service(ready).service(metrics))
            .bind(("0.0.0.0", 8000))
            .unwrap_or_else(|e| {
                tracing::error!(error = e.to_string(), "metrics server error");
                std::process::exit(1);
            })
            .run()
            .map(|f| f.map_err(|e| e.into())),
    );

    (0..4)
        .map(|i| tokio::spawn(run_async_processor(i, sr_settings.clone())))
        .chain(std::iter::once(http_server))
        .collect::<FuturesUnordered<_>>()
        .for_each(|result| async {
            result
                .unwrap_or_else(|e| {
                    tracing::error!(error = e.to_string(), "unable to run worker thread");
                    std::process::exit(1);
                })
                .unwrap_or_else(|e| {
                    tracing::error!(error = e.to_string(), "worker failed");
                    std::process::exit(1);
                });
        })
        .await;
}
