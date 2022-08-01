use avro_rs::from_value;

use cached::lazy_static;
use chrono::{TimeZone, Utc};

use log::{error, info};

use futures::TryStreamExt;

use lazy_static::lazy_static;

use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::error::KafkaError;
use rdkafka::message::OwnedMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;

use schema_registry_converter::async_impl::avro::{AvroDecoder, AvroEncoder};
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;

use std::time::Duration;
use std::{env, format};

use crate::error::Error;
use crate::schemas::{DatasetEvent, DatasetEventType, MQAEvent};
use crate::url::parse_rdf_graph_and_check_urls;

lazy_static! {
    pub static ref BROKERS: String = env::var("BROKERS").unwrap_or("localhost:9092".to_string());
    pub static ref SCHEMA_REGISTRY: String =
        env::var("SCHEMA_REGISTRY").unwrap_or("http://localhost:8081".to_string());
    pub static ref INPUT_TOPIC: String =
        env::var("INPUT_TOPIC").unwrap_or("dataset-events".to_string());
    pub static ref OUTPUT_TOPIC: String =
        env::var("OUTPUT_TOPIC").unwrap_or("mqa-events".to_string());
}

pub fn create_consumer() -> Result<StreamConsumer, KafkaError> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "fdk-mqa-url-checker")
        .set("bootstrap.servers", BROKERS.clone())
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "beginning")
        .set("api.version.request", "false")
        .set("security.protocol", "plaintext")
        .set("debug", "all")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()?;
    consumer.subscribe(&[&INPUT_TOPIC])?;
    Ok(consumer)
}

pub fn create_producer() -> Result<FutureProducer, KafkaError> {
    ClientConfig::new()
        .set("bootstrap.servers", BROKERS.clone())
        .set("message.timeout.ms", "5000")
        .create()
}

/// Creates all the resources and runs the event loop. The event loop will:
///   1) receive a stream of messages from the `StreamConsumer`.
///   2) filter out eventual Kafka errors.
///   3) send the message to a thread pool for processing.
///   4) produce the result to the output topic.
/// `tokio::spawn` is used to handle IO-bound tasks in parallel (e.g., producing
/// the messages).
pub async fn run_async_processor(sr_settings: SrSettings) -> Result<(), Error> {
    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let consumer = create_consumer()?;
    let producer = create_producer()?;

    // Create the outer pipeline on the message stream.
    let stream_processor = consumer.stream().try_for_each(|borrowed_message| {
        let decoder = AvroDecoder::new(sr_settings.clone());
        let mut encoder = AvroEncoder::new(sr_settings.clone());
        let producer = producer.clone();

        async move {
            // Borrowed messages can't outlive the consumer they are received from, so they need to
            // be owned in order to be sent to a separate thread.
            let owned_message = borrowed_message.detach();
            tokio::spawn(async move {
                let mqa_event = handle_dataset_event(owned_message, decoder).await;

                match mqa_event {
                    Ok(Some(evt)) => {
                        let fdk_id = evt.fdk_id.clone();
                        let schema_strategy = SubjectNameStrategy::RecordNameStrategy(
                            String::from("no.fdk.mqa.MQAEvent"),
                        );

                        match encoder.encode_struct(evt, &schema_strategy).await {
                            Ok(encoded_payload) => {
                                let record: FutureRecord<String, Vec<u8>> =
                                    FutureRecord::to(&OUTPUT_TOPIC).payload(&encoded_payload);
                                let produce_future = producer.send(record, Duration::from_secs(0));
                                match produce_future.await {
                                    Ok(delivery) => info!(
                                        "{} - Produce mqa event succeeded {:?}",
                                        fdk_id, delivery
                                    ),
                                    Err((e, _)) => {
                                        error!("{} - Produce mqa event failed {:?}", fdk_id, e)
                                    }
                                }
                            }
                            Err(e) => error!("Encoding message failed: {}", e),
                        }
                    }
                    Ok(None) => info!("Ignoring unknown event type"),
                    Err(e) => error!("Handle dataset-event failed: {}", e),
                }
            });
            Ok(())
        }
    });

    info!("Starting event loop");
    stream_processor.await.expect("stream processing failed");
    info!("Stream processing terminated");

    Ok(())
}

async fn parse_dataset_event(
    msg: OwnedMessage,
    mut decoder: AvroDecoder<'_>,
) -> Result<DatasetEvent, String> {
    match decoder.decode(msg.payload()).await {
        Ok(result) => match result.name {
            Some(name) => match name.name.as_str() {
                "DatasetEvent" => match name.namespace {
                    Some(namespace) => match namespace.as_str() {
                        "no.fdk.dataset" => match from_value::<DatasetEvent>(&result.value) {
                            Ok(event) => Ok(event),
                            Err(e) => Err(format!("Deserialization failed {}", e)),
                        },
                        ns => Err(format!("Unexpected namespace {}", ns)),
                    },
                    None => Err("No namespace in schema, while expected".to_string()),
                },
                name => Err(format!("Unexpected name {}", name)),
            },
            None => Err("No name in schema, while expected".to_string()),
        },
        Err(e) => Err(format!("error getting dataset-event: {}", e)),
    }
}

/// Read DatasetEvent message of type DATASET_HARVESTED
async fn handle_dataset_event(
    msg: OwnedMessage,
    decoder: AvroDecoder<'_>,
) -> Result<Option<MQAEvent>, Error> {
    info!("Handle DatasetEvent on message {}", msg.offset());

    let dataset_event = parse_dataset_event(msg, decoder).await?;

    match dataset_event.event_type {
        DatasetEventType::DatasetHarvested => {
            let dt = Utc.timestamp_millis(dataset_event.timestamp);
            info!(
                "{} - Processing dataset harvested event with timestamp {:?}",
                dataset_event.fdk_id, dt
            );

            parse_rdf_graph_and_check_urls(dataset_event.fdk_id, dataset_event.graph)
                .map(|e| Some(e))
        }
        _ => Ok(None),
    }
}
