use std::time::Duration;

use futures::{stream::FuturesUnordered, StreamExt};
use log::{error, info};
use schema_registry_converter::async_impl::schema_registry::SrSettings;

use crate::{
    kafka::{BROKERS, INPUT_TOPIC, OUTPUT_TOPIC, SCHEMA_REGISTRY},
    schemas::setup_schemas,
};

mod error;
mod kafka;
mod rdf;
mod schemas;
mod url;
mod vocab;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .with_current_span(false)
        .init();

    tracing::info!(
        brokers = BROKERS.to_string(),
        schema_registry = SCHEMA_REGISTRY.to_string(),
        input_topic = INPUT_TOPIC.to_string(),
        output_topic = OUTPUT_TOPIC.to_string(),
        "starting service"
    );

    let mut schema_registry_urls = SCHEMA_REGISTRY.split(",");
    let mut sr_settings_builder =
        SrSettings::new_builder(schema_registry_urls.next().unwrap().to_string());
    schema_registry_urls.for_each(|url| {
        sr_settings_builder.add_url(url.to_string());
    });

    let sr_settings = sr_settings_builder
        .set_timeout(Duration::from_secs(5))
        .build()
        .unwrap_or_else(|_| {
            error!("unable to create SrSettings");
            std::process::exit(1);
        });

    let id = setup_schemas(&sr_settings).await.unwrap_or_else(|_| {
        error!("unable to register schemas");
        std::process::exit(1);
    });
    info!("Schema succesfully registered with id={}", id);

    (0..4)
        .map(|i| tokio::spawn(kafka::run_async_processor(i, sr_settings.clone())))
        .collect::<FuturesUnordered<_>>()
        .for_each(|result| async {
            result
                .unwrap_or_else(|_| {
                    error!("unable to run worker thread");
                    std::process::exit(1);
                })
                .unwrap_or_else(|_| {
                    error!("worker failed");
                    std::process::exit(1);
                });
        })
        .await
}
