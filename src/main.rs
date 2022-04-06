use std::str;
use std::time::Duration;

use clap::{Arg, Command};

use futures::stream::FuturesUnordered;
use futures::StreamExt;

use log::info;

use schema_registry_converter::async_impl::schema_registry::SrSettings;

use crate::schemas::setup_schemas;
use crate::utils::setup_logger;

mod kafka;
mod rdf;
mod schemas;
mod url;
mod utils;
mod vocab;

#[tokio::main]
async fn main() {
    let matches = Command::new("fdk-mqa-url-checker")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("FDK MQA Url checker")
        .arg(
            Arg::new("brokers")
                .short('b')
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::new("group-id")
                .short('g')
                .long("group-id")
                .help("Consumer group id")
                .takes_value(true)
                .default_value("fdk-mqa-url-checker"),
        )
        .arg(
            Arg::new("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace')")
                .takes_value(true),
        )
        .arg(
            Arg::new("input-topic")
                .long("input-topic")
                .help("Input topic")
                .takes_value(true)
                .default_value("dataset-events"),
        )
        .arg(
            Arg::new("output-topic")
                .long("output-topic")
                .help("Output topic")
                .takes_value(true)
                .default_value("mqa-events"),
        )
        .arg(
            Arg::new("num-workers")
                .long("num-workers")
                .help("Number of workers")
                .takes_value(true)
                .default_value("1"),
        )
        .arg(
            Arg::new("schema-registry")
                .long("schema-registry")
                .help("Schema registry')")
                .takes_value(true)
                .default_value("http://localhost:8081"),
        )
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();
    let input_topic = matches.value_of("input-topic").unwrap();
    let output_topic = matches.value_of("output-topic").unwrap();
    let num_workers = matches.value_of_t("num-workers").unwrap();
    let schema_registry = matches.value_of("schema-registry").unwrap();

    info!("Using following settings:");
    info!("  brokers:         {}", brokers);
    info!("  group_id:        {}", group_id);
    info!("  input_topic:     {}", input_topic);
    info!("  output_topic:    {}", output_topic);
    info!("  num_workers:     {}", num_workers);
    info!("  schema_registry: {}", schema_registry);

    let schema_registry_urls = schema_registry.split(",").collect::<Vec<&str>>();
    let mut sr_settings_builder =
        SrSettings::new_builder(schema_registry_urls.first().unwrap().to_string());
    for url in schema_registry_urls.iter().enumerate().skip(1) {
        sr_settings_builder.add_url(url.1.to_string());
    }

    let sr_settings = sr_settings_builder
        .set_timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    setup_schemas(&sr_settings).await;

    (0..num_workers)
        .map(|_| {
            tokio::spawn(kafka::run_async_processor(
                brokers.to_owned(),
                group_id.to_owned(),
                input_topic.to_owned(),
                output_topic.to_owned(),
                sr_settings.to_owned(),
            ))
        })
        .collect::<FuturesUnordered<_>>()
        .for_each(|_| async { () })
        .await
}
