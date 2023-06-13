use std::time::Duration;

use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{CommitMode, Consumer, StreamConsumer},
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
use serde::{de::DeserializeOwned, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    KafkaError(#[from] rdkafka::error::KafkaError),
    #[error(transparent)]
    AvroError(#[from] apache_avro::Error),
    #[error(transparent)]
    SchemaRegistryError(#[from] schema_registry_converter::error::SRCError),
}

/// Creates SrSettings from comma separated string of schema registry urls.
pub fn create_sr_settings(schema_registry_urls: &str) -> Result<SrSettings, Error> {
    let mut urls = schema_registry_urls.split(",");
    let mut sr_settings_builder =
        SrSettings::new_builder(urls.next().unwrap_or_default().to_string());
    urls.for_each(|url| {
        sr_settings_builder.add_url(url.to_string());
    });

    let sr_settings = sr_settings_builder
        .set_timeout(Duration::from_secs(5))
        .build()?;
    Ok(sr_settings)
}

/// Consumes all messages until no more can be received within the timeout period.
pub async fn consume_all_messages(consumer: &StreamConsumer) -> Result<(), Error> {
    loop {
        // Loop untill no nessage can be received within timeout.
        let timeout_duration = Duration::from_millis(500);
        match receive_message(consumer, timeout_duration).await {
            Err(Error::KafkaError(KafkaError::NoMessageReceived)) => return Ok(()),
            Err(e) => return Err(e),
            Ok(_) => (),
        }
    }
}

/// Consumes and returns a single message, if received within the timeout period.
pub async fn receive_message(
    consumer: &StreamConsumer,
    timeout_duration: Duration,
) -> Result<BorrowedMessage, Error> {
    match tokio::time::timeout(timeout_duration, consumer.recv()).await {
        Ok(result) => {
            let message = result?;
            // Commit offset back to kafka.
            consumer.commit_message(&message, CommitMode::Sync)?;
            Ok(message)
        }
        // Timeout while trying to receive new message.
        Err(_) => Err(Error::KafkaError(KafkaError::NoMessageReceived)),
    }
}

pub struct AvroProducer<'a> {
    producer: FutureProducer,
    encoder: AvroEncoder<'a>,
}

impl AvroProducer<'_> {
    /// Creates a new Kafka producer that integrates avro serialization.
    pub fn new(broker_urls: &str, schema_registry_urls: &str) -> Result<Self, Error> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", broker_urls)
            .create::<FutureProducer>()?;

        let sr_settings = create_sr_settings(schema_registry_urls)?;
        let encoder = AvroEncoder::new(sr_settings);

        Ok(Self { producer, encoder })
    }

    /// Encodes and produces a kafka message.
    pub async fn produce<I: Serialize, S: ToString>(
        &mut self,
        topic: &str,
        schema: S,
        item: I,
    ) -> Result<(), Error> {
        let encoded = self
            .encoder
            .encode_struct(
                item,
                &SubjectNameStrategy::RecordNameStrategy(schema.to_string()),
            )
            .await?;
        let record: FutureRecord<String, Vec<u8>> =
            FutureRecord::to(topic.as_ref()).payload(&encoded);
        self.producer
            .send(record, Duration::from_secs(0))
            .await
            .map_err(|e| e.0)?;
        Ok(())
    }
}

pub struct AvroConsumer<'a> {
    consumer: StreamConsumer,
    decoder: AvroDecoder<'a>,
}

impl AvroConsumer<'_> {
    /// Creates a new Kafka consumer that integrates avro serialization.
    pub fn new(broker_urls: &str, schema_registry_urls: &str, topic: &str) -> Result<Self, Error> {
        let consumer = ClientConfig::new()
            .set("group.id", "test")
            .set("bootstrap.servers", broker_urls)
            .set("auto.offset.reset", "beginning")
            .set("security.protocol", "plaintext")
            .set("debug", "all")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create::<StreamConsumer>()?;
        consumer.subscribe(&[topic])?;

        let sr_settings = create_sr_settings(schema_registry_urls)?;
        let decoder = AvroDecoder::new(sr_settings);

        Ok(Self { consumer, decoder })
    }

    /// Consumes all messages until no more can be received within the timeout period.
    pub async fn consume_all_messages(&mut self) -> Result<(), Error> {
        consume_all_messages(&self.consumer).await?;
        Ok(())
    }

    /// Receives and decodes a kafka message.
    pub async fn receive_message<D: DeserializeOwned>(&mut self) -> Result<D, Error> {
        let timeout_duration = Duration::from_millis(3000);
        let message = receive_message(&self.consumer, timeout_duration).await?;
        let decoded = self.decoder.decode(message.payload()).await?;
        let deserialized = apache_avro::from_value::<D>(&decoded.value)?;
        Ok(deserialized)
    }
}
