use std::{
    env,
    time::{Duration, Instant},
};

use apache_avro::schema::Name;
use lazy_static::lazy_static;
use oxigraph::store::Store;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    error::{KafkaError, RDKafkaErrorCode},
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
    kafka_context::ThrottledContext,
    metrics::{PROCESSED_MESSAGES, PROCESSING_TIME},
    schemas::{DatasetEvent, DatasetEventType, InputEvent, MqaEvent, MqaEventType},
    url::parse_rdf_graph_and_check_urls,
};

const CONSUMER_GROUP_ID: &str = "fdk-mqa-url-checker";
const SCHEMA_REGISTRY_TIMEOUT_SECS: u64 = 30;
const SESSION_TIMEOUT_MS: &str = "6000";
const MAX_PARTITION_FETCH_BYTES: &str = "2097152"; // 2 MiB
const MESSAGE_TIMEOUT_MS: &str = "5000";

lazy_static! {
    pub static ref BROKERS: String = env::var("BROKERS").unwrap_or("localhost:9092".to_string());
    pub static ref KAFKA_SECURITY_PROTOCOL: String =
        env::var("KAFKA_SECURITY_PROTOCOL").unwrap_or("plaintext".to_string());
    pub static ref KAFKA_SASL_MECHANISM: String =
        env::var("KAFKA_SASL_MECHANISM").unwrap_or("SCRAM-SHA-512".to_string());
    pub static ref KAFKA_SASL_USERNAME: String =
        env::var("KAFKA_SASL_USERNAME").unwrap_or_default();
    pub static ref KAFKA_SASL_PASSWORD: String =
        env::var("KAFKA_SASL_PASSWORD").unwrap_or_default();
    pub static ref SCHEMA_REGISTRY: String =
        env::var("SCHEMA_REGISTRY").unwrap_or("http://localhost:8081".to_string());
    pub static ref INPUT_TOPIC: String =
        env::var("INPUT_TOPIC").unwrap_or("mqa-dataset-events".to_string());
    pub static ref OUTPUT_TOPIC: String =
        env::var("OUTPUT_TOPIC").unwrap_or("mqa-events".to_string());
}

pub fn create_sr_settings() -> Result<SrSettings, Error> {
    create_sr_settings_from_urls(
        &SCHEMA_REGISTRY,
        Duration::from_secs(SCHEMA_REGISTRY_TIMEOUT_SECS),
    )
}

/// Build schema registry settings from a comma-separated URL list.
pub fn create_sr_settings_from_urls(
    schema_registry_urls: &str,
    timeout: Duration,
) -> Result<SrSettings, Error> {
    let mut urls = schema_registry_urls.split(",");

    let mut sr_settings_builder =
        SrSettings::new_builder(urls.next().unwrap_or_default().to_string());
    urls.for_each(|url| {
        sr_settings_builder.add_url(url.to_string());
    });

    let sr_settings = sr_settings_builder.set_timeout(timeout).build()?;
    Ok(sr_settings)
}

/// Consumer type used throughout the service.
pub type KafkaConsumer = StreamConsumer<ThrottledContext>;
/// Producer type used throughout the service.
pub type KafkaProducer = FutureProducer<ThrottledContext>;

pub fn create_consumer() -> Result<KafkaConsumer, KafkaError> {
    let mut client_config = ClientConfig::new();
    client_config
        .set("group.id", CONSUMER_GROUP_ID)
        .set("bootstrap.servers", BROKERS.clone())
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", SESSION_TIMEOUT_MS)
        .set("enable.auto.commit", "true")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "beginning")
        .set("max.partition.fetch.bytes", MAX_PARTITION_FETCH_BYTES);
    set_security(&mut client_config);
    let consumer: KafkaConsumer = client_config.create_with_context(ThrottledContext)?;
    consumer.subscribe(&[&INPUT_TOPIC])?;
    Ok(consumer)
}

/// Applies the broker security settings from the environment.
///
/// SASL credentials are only set when the security protocol requires them,
/// so local development and tests keep using plaintext.
fn set_security(client_config: &mut ClientConfig) {
    let security_protocol = KAFKA_SECURITY_PROTOCOL.to_lowercase();
    client_config.set("security.protocol", &security_protocol);
    if security_protocol.starts_with("sasl") {
        client_config
            .set("sasl.mechanism", KAFKA_SASL_MECHANISM.clone())
            .set("sasl.username", KAFKA_SASL_USERNAME.clone())
            .set("sasl.password", KAFKA_SASL_PASSWORD.clone());
    }
}

pub fn create_producer() -> Result<KafkaProducer, KafkaError> {
    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", BROKERS.clone())
        .set("message.timeout.ms", MESSAGE_TIMEOUT_MS)
        .set("compression.type", "snappy");
    set_security(&mut client_config);
    client_config.create_with_context(ThrottledContext)
}

/// Creates all the resources and runs the event loop. The event loop will:
///   1) receive a stream of messages from the `StreamConsumer`.
///   2) filter out eventual Kafka errors.
///   3) process each message asynchronously (decode, check URLs, encode).
///   4) produce the result to the output topic.
/// Concurrent workers are started with `tokio::spawn` from the binary entrypoint.
pub async fn run_async_processor(worker_id: usize, sr_settings: SrSettings) -> Result<(), Error> {
    tracing::info!(worker_id, "starting worker");

    let consumer = create_consumer()?;
    let producer = create_producer()?;
    let mut encoder = AvroEncoder::new(sr_settings.clone());
    let mut decoder = AvroDecoder::new(sr_settings);

    tracing::info!(worker_id, "listening for messages");
    let mut errors = ErrorStreak::default();
    loop {
        let message = match consumer.recv().await {
            Ok(message) => {
                errors.reset();
                message
            }
            Err(e) => {
                handle_consumer_error(worker_id, &e, &mut errors, Instant::now())?;
                continue;
            }
        };
        let input_store = Store::new()?;
        let output_store = Store::new()?;
        let span = tracing::span!(
            Level::INFO,
            "message",
            // topic = message.topic(),
            partition = message.partition(),
            offset = message.offset(),
            timestamp = message.timestamp().to_millis(),
        );

        process_received_message(
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

async fn process_received_message(
    consumer: &KafkaConsumer,
    producer: &KafkaProducer,
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
            tracing::debug!(elapsed_millis, "message handled successfully");
            PROCESSED_MESSAGES.with_label_values(&["success"]).inc();

            if let Err(e) = consumer.store_offset_from_message(&message) {
                tracing::warn!(error = e.to_string(), "failed to store offset");
            };
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
}

pub async fn handle_message(
    producer: &KafkaProducer,
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
            let graph =
                parse_rdf_graph_and_check_urls(input_store, output_store, event.graph).await?;
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

/// How often a worker repeats the warning while the broker stays unreachable.
const CONSUMER_ERROR_LOG_INTERVAL: Duration = Duration::from_secs(10);

/// How long a worker puts up with nothing but errors from the consumer before
/// it gives up. A broker restart is over well within this; a wrong broker
/// address or a long outage ends in a restarting pod instead of a silent one.
const CONSUMER_ERROR_DEADLINE: Duration = Duration::from_secs(5 * 60);

/// A pause between errors long enough to count them as separate incidents.
/// librdkafka repeats an error at least every 30 seconds while it lasts.
const CONSUMER_ERROR_STREAK_GAP: Duration = Duration::from_secs(60);

/// A run of consecutive errors from the consumer stream, cleared by the next
/// successfully received message or by a gap of `CONSUMER_ERROR_STREAK_GAP`
/// without errors, so that two short blips on a quiet topic do not add up.
#[derive(Debug, Default)]
struct ErrorStreak {
    started: Option<Instant>,
    last_seen: Option<Instant>,
    last_logged: Option<Instant>,
}

impl ErrorStreak {
    fn reset(&mut self) {
        *self = Self::default();
    }
}

/// Decides what to do with an error from the consumer stream.
///
/// librdkafka reports broker outages, transport failures and failed (re)connects
/// as error events and reconnects on its own, so the worker keeps consuming and
/// logs at most one warning per interval. The worker ends instead when the error
/// is one no amount of retrying will fix, such as a wrong password, a missing
/// ACL or an unknown topic, or when it has seen nothing but errors for
/// `CONSUMER_ERROR_DEADLINE`. Either way the process exits and the pod restarts,
/// which is visible, rather than idling forever.
fn handle_consumer_error(
    worker_id: usize,
    error: &KafkaError,
    streak: &mut ErrorStreak,
    now: Instant,
) -> Result<(), Error> {
    if is_unrecoverable(error) {
        return Err(error.clone().into());
    }
    if streak
        .last_seen
        .is_some_and(|at| now.duration_since(at) > CONSUMER_ERROR_STREAK_GAP)
    {
        streak.reset();
    }
    streak.last_seen = Some(now);
    let started = *streak.started.get_or_insert(now);
    if now.duration_since(started) >= CONSUMER_ERROR_DEADLINE {
        tracing::error!(
            worker_id,
            error = %error,
            seconds = now.duration_since(started).as_secs(),
            "consumer has seen nothing but errors, giving up"
        );
        return Err(error.clone().into());
    }
    if streak
        .last_logged
        .is_none_or(|at| now.duration_since(at) >= CONSUMER_ERROR_LOG_INTERVAL)
    {
        tracing::warn!(
            worker_id,
            error = %error,
            "consumer error, retrying"
        );
        streak.last_logged = Some(now);
    }
    Ok(())
}

/// librdkafka's fatal errors, plus the errors that only change when the
/// configuration does: failed authentication, denied authorization and a
/// subscribed topic that does not exist.
fn is_unrecoverable(error: &KafkaError) -> bool {
    match error {
        KafkaError::MessageConsumptionFatal(_) => true,
        KafkaError::MessageConsumption(code) => matches!(
            code,
            RDKafkaErrorCode::Authentication
                | RDKafkaErrorCode::SaslAuthenticationFailed
                | RDKafkaErrorCode::TopicAuthorizationFailed
                | RDKafkaErrorCode::GroupAuthorizationFailed
                | RDKafkaErrorCode::ClusterAuthorizationFailed
                | RDKafkaErrorCode::UnknownTopicOrPartition
        ),
        _ => false,
    }
}

#[cfg(test)]
mod consumer_error_tests {
    use std::time::{Duration, Instant};

    use rdkafka::error::{KafkaError, RDKafkaErrorCode};

    use super::{
        handle_consumer_error, ErrorStreak, CONSUMER_ERROR_DEADLINE, CONSUMER_ERROR_LOG_INTERVAL,
        CONSUMER_ERROR_STREAK_GAP,
    };

    fn transient() -> KafkaError {
        KafkaError::MessageConsumption(RDKafkaErrorCode::BrokerTransportFailure)
    }

    /// Returns whether the worker stays alive.
    fn handle(error: &KafkaError, streak: &mut ErrorStreak, now: Instant) -> bool {
        handle_consumer_error(0, error, streak, now).is_ok()
    }

    #[test]
    fn transient_errors_keep_the_worker_alive() {
        for code in [
            RDKafkaErrorCode::BrokerTransportFailure,
            RDKafkaErrorCode::AllBrokersDown,
            RDKafkaErrorCode::Resolve,
            RDKafkaErrorCode::NotCoordinator,
        ] {
            let error = KafkaError::MessageConsumption(code);
            let mut streak = ErrorStreak::default();
            assert!(handle(&error, &mut streak, Instant::now()), "{code:?}");
        }
    }

    #[test]
    fn authentication_error_ends_the_worker() {
        for code in [
            RDKafkaErrorCode::Authentication,
            RDKafkaErrorCode::SaslAuthenticationFailed,
        ] {
            let error = KafkaError::MessageConsumption(code);
            let mut streak = ErrorStreak::default();
            assert!(!handle(&error, &mut streak, Instant::now()), "{code:?}");
        }
    }

    #[test]
    fn authorization_error_ends_the_worker() {
        for code in [
            RDKafkaErrorCode::TopicAuthorizationFailed,
            RDKafkaErrorCode::GroupAuthorizationFailed,
            RDKafkaErrorCode::ClusterAuthorizationFailed,
        ] {
            let error = KafkaError::MessageConsumption(code);
            let mut streak = ErrorStreak::default();
            assert!(!handle(&error, &mut streak, Instant::now()), "{code:?}");
        }
    }

    #[test]
    fn unknown_topic_ends_the_worker() {
        let error = KafkaError::MessageConsumption(RDKafkaErrorCode::UnknownTopicOrPartition);
        assert!(!handle(&error, &mut ErrorStreak::default(), Instant::now()));
    }

    #[test]
    fn fatal_error_ends_the_worker() {
        let error = KafkaError::MessageConsumptionFatal(RDKafkaErrorCode::Fatal);
        assert!(!handle(&error, &mut ErrorStreak::default(), Instant::now()));
    }

    #[test]
    fn repeated_errors_are_logged_once_per_interval() {
        let mut streak = ErrorStreak::default();
        let start = Instant::now();

        assert!(handle(&transient(), &mut streak, start));
        assert_eq!(streak.last_logged, Some(start));

        let halfway = start + CONSUMER_ERROR_LOG_INTERVAL / 2;
        assert!(handle(&transient(), &mut streak, halfway));
        assert_eq!(streak.last_logged, Some(start));

        let later = start + CONSUMER_ERROR_LOG_INTERVAL;
        assert!(handle(&transient(), &mut streak, later));
        assert_eq!(streak.last_logged, Some(later));
    }

    #[test]
    fn errors_past_the_deadline_end_the_worker() {
        let mut streak = ErrorStreak::default();
        let start = Instant::now();
        let deadline = start + CONSUMER_ERROR_DEADLINE;

        // librdkafka repeats the error every 30 seconds while the broker is down.
        let mut now = start;
        while now < deadline {
            assert!(handle(&transient(), &mut streak, now));
            now += Duration::from_secs(30);
        }
        assert_eq!(streak.started, Some(start));
        assert!(!handle(&transient(), &mut streak, deadline));
    }

    #[test]
    fn a_quiet_spell_between_errors_restarts_the_deadline() {
        let mut streak = ErrorStreak::default();
        let start = Instant::now();
        let second_blip = start + CONSUMER_ERROR_DEADLINE + CONSUMER_ERROR_STREAK_GAP;

        let first_blip_again = start + Duration::from_secs(30);

        assert!(handle(&transient(), &mut streak, start));
        assert!(handle(&transient(), &mut streak, first_blip_again));

        assert!(handle(&transient(), &mut streak, second_blip));
        assert_eq!(streak.started, Some(second_blip));
    }

    #[test]
    fn a_received_message_restarts_the_deadline() {
        let mut streak = ErrorStreak::default();
        let start = Instant::now();
        let late = start + CONSUMER_ERROR_DEADLINE;

        assert!(handle(&transient(), &mut streak, start));
        streak.reset();

        assert!(handle(&transient(), &mut streak, late));
        assert_eq!(streak.started, Some(late));
    }
}
