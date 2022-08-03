use std::{env, time::Duration};

use avro_rs::from_value;
use lazy_static::lazy_static;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    message::BorrowedMessage,
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Message,
};
use schema_registry_converter::{
    async_impl::{
        avro::{AvroDecoder, AvroEncoder},
        schema_registry::SrSettings,
    },
    schema_registry_common::SubjectNameStrategy,
};
use tracing::{Instrument, Level};

use crate::{
    error::Error,
    schemas::{DatasetEvent, DatasetEventType, MQAEvent, MQAEventType},
    url::parse_rdf_graph_and_check_urls,
};

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
pub async fn run_async_processor(worker_id: usize, sr_settings: SrSettings) -> Result<(), Error> {
    tracing::info!(worker_id, "starting worker");

    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let consumer = create_consumer()?;
    let producer = create_producer()?;
    let mut encoder = AvroEncoder::new(sr_settings.clone());
    let mut decoder = AvroDecoder::new(sr_settings);

    tracing::info!(worker_id, "listening for messages");
    loop {
        let message = consumer.recv().await?;
        let span = tracing::span!(
            Level::INFO,
            "message",
            // topic = message.topic(),
            // partition = message.partition(),
            offset = message.offset(),
            timestamp = message.timestamp().to_millis(),
        );

        receive_message(&consumer, &producer, &mut decoder, &mut encoder, &message)
            .instrument(span)
            .await;
    }
}

async fn receive_message(
    consumer: &StreamConsumer,
    producer: &FutureProducer,
    decoder: &mut AvroDecoder<'_>,
    encoder: &mut AvroEncoder<'_>,
    message: &BorrowedMessage<'_>,
) {
    match handle_message(producer, decoder, encoder, message).await {
        Ok(_) => tracing::info!("message handled successfully"),
        Err(e) => tracing::error!(error = e.to_string(), "failed while handling message"),
    };
    if let Err(e) = consumer.store_offset_from_message(&message) {
        tracing::warn!(error = e.to_string(), "failed to store offset");
    };
}

async fn handle_message(
    producer: &FutureProducer,
    decoder: &mut AvroDecoder<'_>,
    encoder: &mut AvroEncoder<'_>,
    message: &BorrowedMessage<'_>,
) -> Result<(), Error> {
    let mqa_event = handle_dataset_event(decoder, message).await;
    match mqa_event {
        Ok(Some(evt)) => {
            let schema_strategy =
                SubjectNameStrategy::RecordNameStrategy(String::from("no.fdk.mqa.MQAEvent"));

            let encoded_payload = encoder.encode_struct(evt, &schema_strategy).await?;
            let record: FutureRecord<String, Vec<u8>> =
                FutureRecord::to(&OUTPUT_TOPIC).payload(&encoded_payload);
            producer
                .send(record, Duration::from_secs(0))
                .await
                .map_err(|e| e.0)?;
            Ok(())
        }
        Ok(None) => {
            tracing::info!("Ignoring unknown event type");
            Ok(())
        }
        Err(e) => Err(e),
    }
}

async fn parse_dataset_event(
    decoder: &mut AvroDecoder<'_>,
    message: &BorrowedMessage<'_>,
) -> Result<DatasetEvent, String> {
    match decoder.decode(message.payload()).await {
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
    decoder: &mut AvroDecoder<'_>,
    message: &BorrowedMessage<'_>,
) -> Result<Option<MQAEvent>, Error> {
    tracing::info!("Handle DatasetEvent on message");

    let dataset_event = parse_dataset_event(decoder, message).await?;

    match dataset_event.event_type {
        DatasetEventType::DatasetHarvested => {
            tracing::info!("Processing dataset harvested event",);

            parse_rdf_graph_and_check_urls(&dataset_event.fdk_id, dataset_event.graph).map(
                |graph| {
                    Some(MQAEvent {
                        event_type: MQAEventType::UrlsChecked,
                        fdk_id: dataset_event.fdk_id,
                        graph,
                        timestamp: dataset_event.timestamp,
                    })
                },
            )
        }
        _ => Ok(None),
    }
}
