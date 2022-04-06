use avro_rs::from_value;

use chrono::{TimeZone, Utc};

use log::{error, info};

use futures::TryStreamExt;

use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;

use schema_registry_converter::async_impl::avro::{AvroDecoder, AvroEncoder};
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;

use std::format;
use std::time::Duration;

use crate::schemas::{DatasetEvent, DatasetEventType, MQAEvent};
use crate::url::parse_rdf_graph_and_check_urls;

// Creates all the resources and runs the event loop. The event loop will:
//   1) receive a stream of messages from the `StreamConsumer`.
//   2) filter out eventual Kafka errors.
//   3) send the message to a thread pool for processing.
//   4) produce the result to the output topic.
// `tokio::spawn` is used to handle IO-bound tasks in parallel (e.g., producing
// the messages), while `tokio::task::spawn_blocking` is used to handle the
// simulated CPU-bound task.
pub async fn run_async_processor(
    brokers: String,
    group_id: String,
    input_topic: String,
    output_topic: String,
    sr_settings: SrSettings,
) {
    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &group_id)
        .set("bootstrap.servers", &brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "beginning")
        .set("api.version.request", "false")
        .set("security.protocol", "plaintext")
        .set("debug", "all")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[&input_topic])
        .expect("Can't subscribe to specified topic");

    // Create the `FutureProducer` to produce asynchronously.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    // Create the outer pipeline on the message stream.
    let stream_processor = consumer.stream().try_for_each(|borrowed_message| {
        let decoder = AvroDecoder::new(sr_settings.clone());
        let mut encoder = AvroEncoder::new(sr_settings.clone());
        let producer = producer.clone();
        let output_topic = output_topic.to_string();
        async move {
            // Process each message
            record_borrowed_message_receipt(&borrowed_message).await;
            // Borrowed messages can't outlive the consumer they are received from, so they need to
            // be owned in order to be sent to a separate thread.
            let owned_message = borrowed_message.detach();
            record_owned_message_receipt(&owned_message).await;
            tokio::spawn(async move {
                // The body of this block will be executed on the main thread pool,
                // but we perform `expensive_computation` on a separate thread pool
                // for CPU-intensive tasks via `tokio::task::spawn_blocking`.
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
                                    FutureRecord::to(&output_topic).payload(&encoded_payload);
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
}

async fn record_borrowed_message_receipt(msg: &BorrowedMessage<'_>) {
    // Simulate some work that must be done in the same order as messages are
    // received; i.e., before truly parallel processing can begin.
    info!("Message received: {}", msg.offset());
}

async fn record_owned_message_receipt(_msg: &OwnedMessage) {
    // Like `record_borrowed_message_receipt`, but takes an `OwnedMessage`
    // instead, as in a real-world use case  an `OwnedMessage` might be more
    // convenient than a `BorrowedMessage`.
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

// Read DatasetEvent message of type DATASET_HARVESTED
async fn handle_dataset_event(
    msg: OwnedMessage,
    decoder: AvroDecoder<'_>,
) -> Result<Option<MQAEvent>, String> {
    info!("Handle DatasetEvent on message {}", msg.offset());

    let dataset_event = parse_dataset_event(msg, decoder).await;

    match dataset_event {
        Ok(event) => match event.event_type {
            DatasetEventType::DatasetHarvested => {
                let dt = Utc.timestamp_millis(event.timestamp);
                info!(
                    "{} - Processing dataset harvested event with timestamp {:?}",
                    event.fdk_id, dt
                );
                parse_rdf_graph_and_check_urls(event.fdk_id, event.graph)
            }
            _ => Ok(None),
        },
        Err(e) => Err(format!("Unable to decode dataset event: {}", e)),
    }
}
