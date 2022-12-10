use std::{
    env,
    time::{Duration, Instant},
};

use avro_rs::schema::Name;
use lazy_static::lazy_static;
use oxigraph::store::Store;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    message::OwnedMessage,
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
    metrics::PROCESSING_TIME,
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

pub struct Worker<'a> {
    pub consumer: StreamConsumer,
    pub producer: FutureProducer,
    pub decoder: AvroDecoder<'a>,
    pub encoder: AvroEncoder<'a>,
    pub input_store: Store,
    pub output_store: Store,
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

    let mut worker = Worker {
        consumer: create_consumer()?,
        producer: create_producer()?,
        encoder: AvroEncoder::new(sr_settings.clone()),
        decoder: AvroDecoder::new(sr_settings),
        input_store: Store::new()?,
        output_store: Store::new()?,
    };

    tracing::info!(worker_id, "listening for messages");
    loop {
        let message = worker.consumer.recv().await?.detach();
        let span = tracing::span!(
            Level::INFO,
            "message",
            topic = message.topic(),
            partition = message.partition(),
            offset = message.offset(),
            timestamp = message.timestamp().to_millis(),
        );

        receive_message(&mut worker, &message)
            .instrument(span)
            .await;
    }
}

async fn receive_message(worker: &mut Worker<'_>, message: &OwnedMessage) {
    let start_time = Instant::now();
    let (result, metric_label) = handle_message(worker, &message).await;
    let elapsed_seconds = start_time.elapsed().as_secs_f64();

    let status_label = match result {
        Ok(_) => {
            tracing::info!(elapsed_seconds, "message handled successfully");
            "success"
        }
        Err(e) => {
            tracing::error!(
                elapsed_seconds,
                error = e.to_string(),
                "failed while handling message"
            );
            "error"
        }
    };
    PROCESSING_TIME
        .with_label_values(&[status_label, &metric_label])
        .observe(elapsed_seconds);

    if let Err(e) =
        worker
            .consumer
            .store_offset(message.topic(), message.partition(), message.offset())
    {
        tracing::error!(error = e.to_string(), "failed to store offset");
    };
}

pub async fn handle_message(
    worker: &mut Worker<'_>,
    message: &OwnedMessage,
) -> (Result<(), Error>, String) {
    match decode_message(&mut worker.decoder, message).await {
        Ok(InputEvent::DatasetEvent(event)) => {
            let event_type = event.event_type.to_string();
            (handle_dataset_event(worker, event).await, event_type)
        }
        Ok(InputEvent::Unknown { namespace, name }) => {
            tracing::warn!(namespace, name, "skipping unknown event");
            (Ok(()), format!("{}.{}", namespace, name))
        }
        Err(e) => (Err(e), "DecodeError".to_string()),
    }
}

async fn decode_message(
    decoder: &mut AvroDecoder<'_>,
    message: &OwnedMessage,
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
                    InputEvent::DatasetEvent(avro_rs::from_value::<DatasetEvent>(&value)?)
                }
                _ => InputEvent::Unknown { namespace, name },
            };
            Ok(event)
        }
        _ => Err("unable to identify event without namespace and name".into()),
    }
}

async fn handle_dataset_event(worker: &mut Worker<'_>, event: DatasetEvent) -> Result<(), Error> {
    let key = event.fdk_id.clone();
    let span = tracing::span!(
        Level::INFO,
        "event",
        fdk_id = event.fdk_id,
        event_type = event.event_type.to_string(),
    );
    let mqa_event = process_dataset_event(&worker.input_store, &worker.output_store, event)
        .instrument(span)
        .await?;

    let encoded = worker
        .encoder
        .encode_struct(
            mqa_event,
            &SubjectNameStrategy::RecordNameStrategy("no.fdk.mqa.MQAEvent".to_string()),
        )
        .await?;

    let record: FutureRecord<String, Vec<u8>> =
        FutureRecord::to(&OUTPUT_TOPIC).key(&key).payload(&encoded);
    worker
        .producer
        .send(record, Duration::from_secs(0))
        .await
        .map_err(|e| e.0)?;

    Ok(())
}

async fn process_dataset_event(
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
