use futures::{stream::FuturesUnordered, StreamExt};
use log::info;

use crate::{
    kafka::{BROKERS, INPUT_TOPIC, OUTPUT_TOPIC, SCHEMA_REGISTRY, SR_SETTINGS},
    schemas::setup_schemas,
    utils::setup_logger,
};

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

    setup_schemas(&SR_SETTINGS).await;

    (0..4)
        .map(|_| tokio::spawn(kafka::run_async_processor()))
        .collect::<FuturesUnordered<_>>()
        .for_each(|_| async { () })
        .await
}
