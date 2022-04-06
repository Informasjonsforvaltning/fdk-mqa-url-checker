use log::{info, error};

use oxigraph::io::{GraphFormat};
use oxigraph::store::{Store, QuadIter, SerializerError};
use oxigraph::model::*;
use oxigraph::model::vocab::{rdf, xsd};

use std::collections::HashMap;

const DCT_FORMAT: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/dc/terms/format");

const DCAT_DATASET_CLASS: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/ns/dcat#Dataset");        

const DCAT_DISTRIBUTION: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/ns/dcat#distribution");        

const DCAT_ACCESS_URL: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/ns/dcat#accessURL");        

const DCAT_DOWNLOAD_URL: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/ns/dcat#downloadURL");    
    
const DQV_QUALITY_MEASUREMENT_CLASS: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/ns/dqv#QualityMeasurement");   

const DQV_HAS_QUALITY_MEASUREMENT: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/ns/dqv#hasQualityMeasurement");    

const DQV_IS_MEASUREMENT_OF: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/ns/dqv#isMeasurementOf");  
    
const DQV_COMPUTED_ON: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/ns/dqv#computedOn"); 

const DQV_VALUE: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/ns/dqv#value"); 

const DCAT_MQA_ACCESS_URL_STATUS_CODE: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("https://data.norge.no/vocabulary/dcatno-mqa#mqa:accessUrlStatusCode");   
        
const DCAT_MQA_DOWNLOAD_URL_STATUS_CODE: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("https://data.norge.no/vocabulary/dcatno-mqa#mqa:downloadUrlStatusCode");

// Parse Turtle RDF and load into store
pub fn parse_turtle(turtle: String) -> Store {
    info!("Loading turtle graph");

    let store = Store::new().unwrap();
    match store.load_graph(turtle.as_ref(), GraphFormat::Turtle, GraphNameRef::DefaultGraph, None) {
        Ok(_) => info!("Graph loaded successfully"),
        Err(e) => error!("Loading graph failed {}", e)
    }

    return store;
}

// Retrieve datasets
pub fn list_datasets(store: &Store) -> QuadIter {
    store.quads_for_pattern(None, Some(rdf::TYPE), Some(DCAT_DATASET_CLASS.into()), None)
}

// Retrieve distributions of a dataset
pub fn list_distributions(dataset: &NamedNode, store: &Store) -> QuadIter {
    store.quads_for_pattern(Some((dataset).into()), Some((DCAT_DISTRIBUTION).into()), None, None)
}

// Retrieve access urls of a distribution
pub fn list_access_urls(distribution: &NamedOrBlankNode, store: &Store) -> QuadIter {
    store.quads_for_pattern(Some((distribution).into()), Some((DCAT_ACCESS_URL).into()), None, None)
}

// Retrieve download urls of a distribution
pub fn list_download_urls(distribution: &NamedOrBlankNode, store: &Store) -> QuadIter {
    store.quads_for_pattern(Some((distribution).into()), Some((DCAT_DOWNLOAD_URL).into()), None, None)
}

// Retrieve distribution formats
pub fn list_formats(distribution: &NamedOrBlankNode, store: &Store) -> QuadIter {
    store.quads_for_pattern(Some((distribution).into()), Some((DCT_FORMAT).into()), None, None)
}

// Retrieve dataset namednode
pub fn get_dataset_node(store: &Store) -> Option<NamedNode> {
    match list_datasets(&store).next() {
        Some(d) => {
            match d {
                Ok(d) => {
                    match d.subject {
                        Subject::NamedNode(n) => Some(n),
                        _ => None                      
                    }
                },
                Err(_) => None
            }            
        },
        None => None       
    }
}

// Map GEO location method
fn map_format_to_head(format_uri: String) -> String {
    let fmt = format_uri.split("/").last().unwrap();
    match fmt {
        "WMS_SRVC" => "WMS".to_string(),
        "WFS_SRVC" => "WFS".to_string(),
        "WCS_SRVC" => "WCS".to_string(),
        _ => "HEAD".to_string()
    }
}

// Extract accessURLs and downloadURLs from dataset
pub fn extract_urls_from_distribution(dist_node: &NamedOrBlankNode, store: &Store) -> Vec<HashMap<String, String>> {    
    let mut urls = Vec::new();            
        
    let mut url_extract = HashMap::new();
    url_extract.insert("distribution_uri".to_string(), dist_node.to_string());

    // Map format to HEAD
    let head = list_formats(&dist_node, store).next()
        .map_or_else(|| "HEAD".to_string(), |fmt| map_format_to_head(fmt.unwrap().to_string()));            
    
    for acc_url in list_access_urls(&dist_node, store) {
        match acc_url.unwrap().object {
            Term::NamedNode(acc_url_node) => { 
                let mut new_url_extract = url_extract.clone();
                new_url_extract.insert("method".to_string(), head.to_string());
                new_url_extract.insert("access_url".to_string(), acc_url_node.into_string());
                urls.push(new_url_extract);
            },
            node => error!("Access URL node is not a NamedNode but {}", node)   
        }
    }

    for dl_url in list_download_urls(&dist_node, store) {
        match dl_url.unwrap().object {
            Term::NamedNode(dl_url_node) => { 
                let mut new_url_extract = url_extract.clone();
                new_url_extract.insert("method".to_string(), head.to_string());
                new_url_extract.insert("access_url".to_string(), dl_url_node.into_string());
                urls.push(new_url_extract);
            },
            node => error!("Download URL node is not a NamedNode but {}", node)   
        }
    }     

    urls
}

// Create new memory metrics store for supplied dataset
pub fn create_metrics_store(dataset: &NamedNode, distributions: &Vec<Quad>) -> Store {
    let store = Store::new().unwrap();

    // Insert dataset 
    store.insert(&Quad::new(dataset.clone(), rdf::TYPE, DCAT_DATASET_CLASS, GraphName::DefaultGraph)).unwrap();
    
    // Insert distributions
    for dist in distributions { 
        store.insert(&Quad::new(dist.subject.clone(), dist.predicate.clone(), dist.object.clone(), GraphName::DefaultGraph)).unwrap();
    }

    store
}

// Add MQA access url status code metric 
pub fn add_access_url_status_metric(distibution: &NamedOrBlankNode, url: String, status_code: u16, store: &Store) {
    add_quality_measurement(distibution, DCAT_MQA_ACCESS_URL_STATUS_CODE, url, status_code, store)
}

// Add MQA download url status code metric 
pub fn add_download_url_status_metric(distibution: &NamedOrBlankNode, url: String, status_code: u16, store: &Store) {
    add_quality_measurement(distibution, DCAT_MQA_DOWNLOAD_URL_STATUS_CODE, url, status_code, store)
}

// Add quality measurement to metric store 
pub fn add_quality_measurement(subject: &NamedOrBlankNode, metric: NamedNodeRef, url: String, value: u16, store: &Store) {
    let measurement = BlankNode::default();
    let value_term = Term::Literal(Literal::new_typed_literal(format!("{}", value), xsd::INTEGER));
    match NamedNode::new(url) {
        Ok(computed_on) => {
            store.insert(&Quad::new(measurement.as_ref(), rdf::TYPE, DQV_QUALITY_MEASUREMENT_CLASS, GraphName::DefaultGraph)).unwrap();
            store.insert(&Quad::new(measurement.as_ref(), DQV_IS_MEASUREMENT_OF, metric, GraphName::DefaultGraph)).unwrap();
            store.insert(&Quad::new(measurement.as_ref(), DQV_COMPUTED_ON, computed_on, GraphName::DefaultGraph)).unwrap();
            store.insert(&Quad::new(measurement.as_ref(), DQV_VALUE, value_term, GraphName::DefaultGraph)).unwrap();
            store.insert(&Quad::new(subject.clone(), DQV_HAS_QUALITY_MEASUREMENT, measurement.as_ref(), GraphName::DefaultGraph)).unwrap();
        },
        Err(e) => error!("Url could not be converted to NamedNode {}", e)
    }    
}

// Dump graph as turtle string
pub fn dump_graph_as_turtle(store: &Store) -> Result<Vec<u8>, SerializerError> {
    let mut buffer = Vec::new();
    store.dump_graph(&mut buffer, GraphFormat::Turtle, GraphNameRef::DefaultGraph)?;
    Ok(buffer)
}

#[allow(dead_code)]
fn main() {
    println!("This is not an executable");
}
