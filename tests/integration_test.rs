use std::time::Duration;

use fdk_mqa_url_checker::{
    error::Error,
    kafka::{
        create_consumer, create_producer, create_sr_settings, handle_message, Worker, BROKERS,
        INPUT_TOPIC, OUTPUT_TOPIC, SCHEMA_REGISTRY,
    },
    schemas::{DatasetEvent, DatasetEventType, MqaEvent},
};
use kafka_utils::{consume_all_messages, receive_message, AvroProducer};
use oxigraph::store::Store;
use rdkafka::consumer::StreamConsumer;
use schema_registry_converter::async_impl::avro::{AvroDecoder, AvroEncoder};
use uuid::Uuid;

use crate::kafka_utils::AvroConsumer;

mod kafka_utils;

#[tokio::test]
async fn test() {
    assert_transformation(
        include_str!("data/dataset_event.ttl"),
        include_str!("data/mqa_event.ttl"),
    )
    .await;
}

pub async fn process_single_message(consumer: StreamConsumer) -> (Result<(), Error>, String) {
    let mut worker = Worker {
        producer: create_producer().unwrap(),
        consumer: create_consumer().unwrap(),
        encoder: AvroEncoder::new(create_sr_settings().unwrap()),
        decoder: AvroDecoder::new(create_sr_settings().unwrap()),
        input_store: Store::new().unwrap(),
        output_store: Store::new().unwrap(),
    };

    let timeout_duration = Duration::from_millis(3000);
    let message = receive_message(&consumer, timeout_duration)
        .await
        .expect("no message received within timeout duration");

    handle_message(&mut worker, &message).await
}

async fn assert_transformation(input: &str, output: &str) {
    let consumer = create_consumer().unwrap();
    // Clear topic of all existing messages.
    consume_all_messages(&consumer).await.unwrap();
    // Start async url-checker process.
    let processor = process_single_message(consumer);

    // Create MQA test event.
    let uuid = Uuid::new_v4();
    let input_message = DatasetEvent {
        event_type: DatasetEventType::DatasetHarvested,
        timestamp: 1647698566000,
        fdk_id: uuid.to_string(),
        graph: input.to_string(),
    };

    // Create consumer and consume all existing messages on output topic.
    let mut consumer = AvroConsumer::new(&BROKERS, &SCHEMA_REGISTRY, &OUTPUT_TOPIC).unwrap();
    consumer.consume_all_messages().await.unwrap();

    // Produce new message to input topic.
    AvroProducer::new(&BROKERS, &SCHEMA_REGISTRY)
        .unwrap()
        .produce(&INPUT_TOPIC, "no.fdk.mqa.DatasetEvent", &input_message)
        .await
        .unwrap();

    // Wait for url-checker to process message.
    let (result, _) = processor.await;
    assert!(result.is_ok());

    // Consume message produced by url-checker.
    let message = consumer.receive_message::<MqaEvent>().await.unwrap();

    assert_eq!(
        sorted_lines(replace_blank(&message.graph)),
        sorted_lines(replace_blank(&output))
    );
}

pub fn replace_blank(text: &str) -> String {
    let mut chars = text.chars().collect::<Vec<char>>();
    for i in (0..(chars.len() - 2)).rev() {
        if chars[i] == '_' && chars[i + 1] == ':' {
            while chars[i] != ' ' {
                chars.remove(i);
            }
            chars.insert(i, 'b')
        }
    }
    chars.iter().collect::<String>()
}

pub fn sorted_lines(text: String) -> Vec<String> {
    let mut lines: Vec<String> = text
        .split("\n")
        .map(|l| l.trim().to_string())
        .filter(|l| l.len() > 0)
        .collect();
    lines.sort();
    lines
}
