use std::{
    env,
    time::{Duration, Instant},
};

use apache_avro::schema::Name;
use lazy_static::lazy_static;
use oxigraph::store::Store;
use rdkafka::{
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
    avro_common::DecodeResult,
    schema_registry_common::SubjectNameStrategy,
};
use tracing::{Instrument, Level};

use crate::{
    error::Error,
    metrics::{PROCESSED_MESSAGES, PROCESSING_TIME},
    schemas::{DatasetEvent, DatasetEventType, InputEvent, MqaEvent, MqaEventType},
    url::parse_rdf_graph_and_check_urls,
};

lazy_static! {
    pub static ref BROKERS: String = env::var("BROKERS").unwrap_or("localhost:9092".to_string());
    pub static ref SCHEMA_REGISTRY: String =
        env::var("SCHEMA_REGISTRY").unwrap_or("http://localhost:8081".to_string());
    pub static ref INPUT_TOPIC: String =
        env::var("INPUT_TOPIC").unwrap_or("mqa-dataset-events".to_string());
    pub static ref OUTPUT_TOPIC: String =
        env::var("OUTPUT_TOPIC").unwrap_or("mqa-events".to_string());
}

pub fn create_sr_settings() -> Result<SrSettings, Error> {
    let mut schema_registry_urls = SCHEMA_REGISTRY.split(",");

    let mut sr_settings_builder =
        SrSettings::new_builder(schema_registry_urls.next().unwrap_or_default().to_string());
    schema_registry_urls.for_each(|url| {
        sr_settings_builder.add_url(url.to_string());
    });

    let sr_settings = sr_settings_builder
        .set_timeout(Duration::from_secs(5))
        .build()?;
    Ok(sr_settings)
}

pub fn create_consumer() -> Result<StreamConsumer, KafkaError> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "fdk-mqa-url-checker")
        .set("bootstrap.servers", BROKERS.clone())
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "beginning")
        .set("api.version.request", "false")
        .set("security.protocol", "plaintext")
        .set("max.partition.fetch.bytes", "2097152")
        .create()?;
    consumer.subscribe(&[&INPUT_TOPIC])?;
    Ok(consumer)
}

pub fn create_producer() -> Result<FutureProducer, KafkaError> {
    ClientConfig::new()
        .set("bootstrap.servers", BROKERS.clone())
        .set("message.timeout.ms", "5000")
        .set("compression.type", "snappy")
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

    let consumer = create_consumer()?;
    let producer = create_producer()?;
    let mut encoder = AvroEncoder::new(sr_settings.clone());
    let mut decoder = AvroDecoder::new(sr_settings);
    let input_store = Store::new()?;
    let output_store = Store::new()?;

    tracing::info!(worker_id, "listening for messages");
    loop {
        let message = consumer.recv().await?;
        let span = tracing::span!(
            Level::INFO,
            "message",
            // topic = message.topic(),
            partition = message.partition(),
            offset = message.offset(),
            timestamp = message.timestamp().to_millis(),
        );

        receive_message(
            &consumer,
            &producer,
            &mut decoder,
            &mut encoder,
            &input_store,
            &output_store,
            &message,
        )
        .instrument(span)
        .await;
    }
}

async fn receive_message(
    consumer: &StreamConsumer,
    producer: &FutureProducer,
    decoder: &mut AvroDecoder<'_>,
    encoder: &mut AvroEncoder<'_>,
    input_store: &Store,
    output_store: &Store,
    message: &BorrowedMessage<'_>,
) {
    let start_time = Instant::now();
    let result = handle_message(
        producer,
        decoder,
        encoder,
        input_store,
        output_store,
        message,
    )
    .await;
    let elapsed_millis = start_time.elapsed().as_millis();
    match result {
        Ok(_) => {
            tracing::info!(elapsed_millis, "message handled successfully");
            PROCESSED_MESSAGES.with_label_values(&["success"]).inc();
        }
        Err(e) => {
            tracing::error!(
                elapsed_millis,
                error = e.to_string(),
                "failed while handling message"
            );
            PROCESSED_MESSAGES.with_label_values(&["error"]).inc();
        }
    };
    PROCESSING_TIME.observe(elapsed_millis as f64 / 1000.0);
    if let Err(e) = consumer.store_offset_from_message(&message) {
        tracing::warn!(error = e.to_string(), "failed to store offset");
    };
}

pub async fn handle_message(
    producer: &FutureProducer,
    decoder: &mut AvroDecoder<'_>,
    encoder: &mut AvroEncoder<'_>,
    input_store: &Store,
    output_store: &Store,
    message: &BorrowedMessage<'_>,
) -> Result<(), Error> {
    match decode_message(decoder, message).await? {
        InputEvent::DatasetEvent(event) => {
            let span = tracing::span!(
                Level::INFO,
                "event",
                fdk_id = event.fdk_id,
                event_type = format!("{:?}", event.event_type),
            );

            let key = event.fdk_id.clone();
            let mqa_event = handle_dataset_event(input_store, output_store, event)
                .instrument(span)
                .await?;

            let encoded = encoder
                .encode_struct(
                    mqa_event,
                    &SubjectNameStrategy::RecordNameStrategy("no.fdk.mqa.MQAEvent".to_string()),
                )
                .await?;

            let record: FutureRecord<String, Vec<u8>> =
                FutureRecord::to(&OUTPUT_TOPIC).key(&key).payload(&encoded);
            producer
                .send(record, Duration::from_secs(0))
                .await
                .map_err(|e| e.0)?;
        }
        InputEvent::Unknown { namespace, name } => {
            tracing::warn!(namespace, name, "skipping unknown event");
        }
    }
    Ok(())
}

async fn decode_message(
    decoder: &mut AvroDecoder<'_>,
    message: &BorrowedMessage<'_>,
) -> Result<InputEvent, Error> {
    match decoder.decode(message.payload()).await? {
        DecodeResult {
            name:
                Some(Name {
                    name,
                    namespace: Some(namespace),
                    ..
                }),
            value,
        } => {
            let event = match (namespace.as_str(), name.as_str()) {
                ("no.fdk.mqa", "DatasetEvent") => {
                    InputEvent::DatasetEvent(apache_avro::from_value::<DatasetEvent>(&value)?)
                }
                _ => InputEvent::Unknown { namespace, name },
            };
            Ok(event)
        }
        _ => Err("unable to identify event without namespace and name".into()),
    }
}

async fn handle_dataset_event(
    input_store: &Store,
    output_store: &Store,
    event: DatasetEvent,
) -> Result<MqaEvent, Error> {
    match event.event_type {
        DatasetEventType::DatasetHarvested => {
            let graph = parse_rdf_graph_and_check_urls(input_store, output_store, event.graph)?;
            Ok(MqaEvent {
                event_type: MqaEventType::UrlsChecked,
                fdk_id: event.fdk_id,
                graph,
                timestamp: event.timestamp,
            })
        }
        DatasetEventType::Unknown => Err(format!("unknown DatasetEventType").into()),
    }
}
