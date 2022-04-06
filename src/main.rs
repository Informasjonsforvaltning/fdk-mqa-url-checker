use std::time::Duration;

use futures::stream::FuturesUnordered;
use futures::StreamExt;

use log::info;

use schema_registry_converter::async_impl::schema_registry::SrSettings;

use crate::kafka::{BROKERS, INPUT_TOPIC, OUTPUT_TOPIC, SCHEMA_REGISTRY};
use crate::schemas::setup_schemas;
use crate::utils::setup_logger;

mod error;
mod kafka;
mod rdf;
mod schemas;
mod url;
mod utils;
mod vocab;

#[tokio::main]
async fn main() {
    setup_logger(true, None);

    info!("Using following settings:");
    info!("  brokers:         {}", BROKERS.to_string());
    info!("  input_topic:     {}", INPUT_TOPIC.to_string());
    info!("  output_topic:    {}", OUTPUT_TOPIC.to_string());
    info!("  schema_registry: {}", SCHEMA_REGISTRY.to_string());

    let mut schema_registry_urls = SCHEMA_REGISTRY.split(",");
    let mut sr_settings_builder =
        SrSettings::new_builder(schema_registry_urls.next().unwrap().to_string());
    schema_registry_urls.for_each(|url| {
        sr_settings_builder.add_url(url.to_string());
    });

    let sr_settings = sr_settings_builder
        .set_timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    setup_schemas(&sr_settings).await;

    (0..4)
        .map(|_| tokio::spawn(kafka::run_async_processor(sr_settings.clone())))
        .collect::<FuturesUnordered<_>>()
        .for_each(|_| async { () })
        .await
}
