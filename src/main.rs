use std::format;
use std::str;
use std::time::Duration;

use clap::{Command, Arg};

use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};

use log::{info, error};

use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;

use avro_rs::{from_value};
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use schema_registry_converter::blocking::avro::{AvroEncoder, AvroDecoder};
use schema_registry_converter::blocking::schema_registry::SrSettings;

use chrono::{TimeZone, Utc};

use oxigraph::model::*;

use crate::utils::{setup_logger, log_error_and_return_none};
use crate::url::{check_url, UrlType};
use crate::schemas::{setup_schemas, DatasetEvent, MQAEvent, MQAEventType};
use crate::rdf::{
    parse_turtle, 
    list_distributions,
    get_dataset_node, 
    extract_urls_from_distribution, 
    create_metrics_store, 
    add_access_url_status_metric,
    add_download_url_status_metric,
    dump_graph_as_turtle};

mod utils;
mod url;
mod schemas;
mod rdf;

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

// Read DatasetEvent message of type DATASET_HARVESTED
fn handle_dataset_event<'a>(msg: OwnedMessage, mut decoder: AvroDecoder) -> Option<MQAEvent> {
    info!("Handle DatasetEvent on message {}", msg.offset());

    let dataset_event: Option<DatasetEvent> = match decoder.decode(msg.payload()) {
    Ok(result) => {            
        match result.name {
        Some(name) => {
            match &name.name[..] {
            "DatasetEvent" => {
                match name.namespace {
                Some(namespace) => {
                    match namespace.as_str() {
                        "no.fdk.dataset" => match from_value::<DatasetEvent>(&result.value) {
                            Ok(event) => Some(event),
                            Err(e) => log_error_and_return_none(format!("Deserialization failed {}", e).as_str())
                        } ,
                        ns => log_error_and_return_none(format!("Unexpected namespace {}", ns).as_str())
                    }
                }
                None => log_error_and_return_none("No namespace in schema, while expected"), 
                } 
            }
            name => log_error_and_return_none(format!("Unexpected name {}", name).as_str()),
            } 
        }
        None => log_error_and_return_none("No name in schema, while expected"),
        }
    }
    Err(e) => log_error_and_return_none(format!("error getting dataset-event: {}", e).as_str()),
    };

    match dataset_event {
        Some(event) => {
            let dt = Utc.timestamp_millis(event.timestamp);
            info!("{} - Processing dataset event with timestamp {:?}", event.fdkId, dt);
            
            let store = parse_turtle(event.graph);

            match get_dataset_node(&store) {
                Some(dataset_node) => {

                    match list_distributions(&dataset_node, &store).collect::<Result<Vec<Quad>,_>>() {
                        Ok(distributions) => {
                            // Make MQA metrics model (DQV)
                            let metrics_store = create_metrics_store(&dataset_node, &distributions);

                            for dist in distributions {       
                                let dist_node = match dist.object {
                                    Term::NamedNode(n) => Some(NamedOrBlankNode::NamedNode(n)),
                                    Term::BlankNode(n) => Some(NamedOrBlankNode::BlankNode(n)),
                                    _ => None
                                }.unwrap();     
                                
                                info!("{} - Extracting urls from distribution", event.fdkId);
                                let urls = extract_urls_from_distribution(&dist_node, &store);
                                info!("{} - Number of urls found {}", event.fdkId, urls.len());

                                for url in urls {
                                    let result = check_url(
                                        url.get("method").unwrap().to_string(), 
                                        if url.contains_key("access_url") {
                                            url.get("access_url").unwrap().to_string()
                                        } else {
                                            url.get("download_url").unwrap().to_string()
                                        },
                                        if url.contains_key("access_url") {
                                            UrlType::ACCESS_URL
                                        } else {
                                            UrlType::DOWNLOAD_URL
                                        });

                                    
                                    println!("{:?}", result);
                                    
                                    match result.url_type {
                                        UrlType::ACCESS_URL => add_access_url_status_metric(&dist_node, result.url, result.status, &metrics_store),
                                        UrlType::DOWNLOAD_URL => add_download_url_status_metric(&dist_node, result.url, result.status, &metrics_store)
                                    }
                                }
                            }

                            // Create MQA event and send to kafka topic
                            return Some(
                                MQAEvent {
                                    r#type: MQAEventType::URLS_CHECKED,
                                    fdkId: event.fdkId,
                                    graph: str::from_utf8(dump_graph_as_turtle(&metrics_store).unwrap().as_slice()).unwrap().to_string(),
                                    timestamp: Utc::now().timestamp_millis()
                                }
                            );   
                        },
                        Err(e) => error!("Listing distributions failed {}", e)
                    }

                    
                    
                },
                None => error!("{} - Dataset node not found in graph", event.fdkId)
            }
            
        },
        None => error!("Unable to decode dataset event")
    }     

    None
}

// Creates all the resources and runs the event loop. The event loop will:
//   1) receive a stream of messages from the `StreamConsumer`.
//   2) filter out eventual Kafka errors.
//   3) send the message to a thread pool for processing.
//   4) produce the result to the output topic.
// `tokio::spawn` is used to handle IO-bound tasks in parallel (e.g., producing
// the messages), while `tokio::task::spawn_blocking` is used to handle the
// simulated CPU-bound task.
async fn run_async_processor(
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
                let mqa_event = tokio::task::spawn_blocking(|| handle_dataset_event(owned_message, decoder))
                        .await
                        .expect("failed to wait for handle dataset-event");
                
                match mqa_event {
                    Some(evt) => {
                        let fdk_id = evt.fdkId.clone();
                        let schema_strategy = SubjectNameStrategy::RecordNameStrategy(String::from("no.fdk.mqa.MQAEvent"));
                        let encoded_payload = encoder.encode_struct(evt, &schema_strategy).unwrap();
                        let record: FutureRecord<String, Vec<u8>> = FutureRecord::to(&output_topic).payload(&encoded_payload);
                        let produce_future = producer.send(record, Duration::from_secs(0));                        
                        match produce_future.await {
                            Ok(delivery) => info!("{} - Produce mqa event succeeded {:?}", fdk_id.clone(), delivery),
                            Err((e, _)) => error!("{} - Produce mqa event failed {:?}", fdk_id.clone(), e),
                        }        
                    },
                    None => ()
                }
                
            });
            Ok(())
        }
    });

    info!("Starting event loop");
    stream_processor.await.expect("stream processing failed");
    info!("Stream processing terminated");
}

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

    let sr_settings = SrSettings::new(String::from(schema_registry));

    setup_schemas(&sr_settings);

    (0..num_workers)
        .map(|_| {
            tokio::spawn(run_async_processor(
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