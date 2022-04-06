use log::{info, warn};

use oxigraph::io::GraphFormat;
use oxigraph::model::vocab::{rdf, xsd};
use oxigraph::model::*;
use oxigraph::store::{QuadIter, SerializerError, Store};

use crate::url::{UrlCheck, UrlType};
use crate::vocab::{dcat, dcat_mqa, dcterms, dqv};

use crate::error::Error;

/// Parse Turtle RDF and load into store
pub fn parse_turtle(turtle: String) -> Result<Store, Error> {
    info!("Loading turtle graph");

    let store = Store::new()?;
    store.load_graph(
        turtle.as_ref(),
        GraphFormat::Turtle,
        GraphNameRef::DefaultGraph,
        None,
    )?;
    Ok(store)
}

/// Retrieve datasets
pub fn list_datasets(store: &Store) -> QuadIter {
    store.quads_for_pattern(
        None,
        Some(rdf::TYPE),
        Some(dcat::DATASET_CLASS.into()),
        None,
    )
}

/// Retrieve distributions of a dataset
pub fn list_distributions(dataset: NamedNodeRef, store: &Store) -> QuadIter {
    store.quads_for_pattern(
        Some(dataset.into()),
        Some(dcat::DISTRIBUTION.into()),
        None,
        None,
    )
}

/// Retrieve access urls of a distribution
pub fn list_access_urls(distribution: NamedOrBlankNodeRef, store: &Store) -> QuadIter {
    store.quads_for_pattern(
        Some(distribution.into()),
        Some(dcat::ACCESS_URL.into()),
        None,
        None,
    )
}

/// Retrieve download urls of a distribution
pub fn list_download_urls(distribution: NamedOrBlankNodeRef, store: &Store) -> QuadIter {
    store.quads_for_pattern(
        Some(distribution.into()),
        Some(dcat::DOWNLOAD_URL.into()),
        None,
        None,
    )
}

/// Retrieve distribution formats
pub fn list_formats(distribution: NamedOrBlankNodeRef, store: &Store) -> QuadIter {
    store.quads_for_pattern(
        Some(distribution.into()),
        Some(dcterms::FORMAT.into()),
        None,
        None,
    )
}

/// Retrieve dataset namednode
pub fn get_dataset_node(store: &Store) -> Option<NamedNode> {
    list_datasets(&store).next().and_then(|d| match d {
        Ok(Quad {
            subject: Subject::NamedNode(n),
            ..
        }) => Some(n),
        _ => None,
    })
}

/// Map GEO location method
fn map_format_to_head(format_uri: String) -> String {
    let fmt = format_uri.split("/").last().unwrap_or_default();
    match fmt {
        "WMS_SRVC" => "WMS",
        "WFS_SRVC" => "WFS",
        "WCS_SRVC" => "WCS",
        _ => "HEAD",
    }
    .to_string()
}

/// Extract accessURLs and downloadURLs from dataset
pub fn extract_urls_from_distribution(
    dist_node: NamedOrBlankNodeRef,
    store: &Store,
) -> Result<Vec<UrlCheck>, Error> {
    let mut urls = Vec::new();

    // Map format to HEAD
    let head = list_formats(dist_node, store)
        .next()
        .map_or("HEAD".to_string(), |fmt_res| {
            map_format_to_head(fmt_res.map_or("".to_string(), |fmt| fmt.object.to_string()))
        });

    for acc_url_result in list_access_urls(dist_node, store) {
        match acc_url_result?.object {
            Term::NamedNode(acc_url_node) => {
                urls.push(UrlCheck {
                    method: head.to_string(),
                    url: acc_url_node.into_string(),
                    url_type: UrlType::AccessUrl,
                });
            }
            node => warn!("Access URL node is not a NamedNode but {}", node),
        }
    }

    for dl_url_result in list_download_urls(dist_node, store) {
        match dl_url_result?.object {
            Term::NamedNode(dl_url_node) => {
                urls.push(UrlCheck {
                    method: head.to_string(),
                    url: dl_url_node.into_string(),
                    url_type: UrlType::DownloadUrl,
                });
            }
            node => warn!("Download URL node is not a NamedNode but {}", node),
        }
    }

    Ok(urls)
}

/// Create new memory metrics store for supplied dataset
pub fn create_metrics_store(dataset: NamedNodeRef) -> Result<Store, Error> {
    let store = Store::new()?;

    // Insert dataset
    store.insert(
        Quad::new(
            dataset.clone(),
            rdf::TYPE,
            dcat::DATASET_CLASS,
            GraphName::DefaultGraph,
        )
        .as_ref(),
    )?;
    Ok(store)
}

/// Add MQA url status code metric
pub fn add_url_status_metric(
    distribution: NamedOrBlankNodeRef,
    url: NamedNodeRef,
    url_type: UrlType,
    status_code: u16,
    store: &Store,
) -> Result<BlankNode, Error> {
    let (predicate, metric) = match url_type {
        UrlType::AccessUrl => (dcat::ACCESS_URL, dcat_mqa::ACCESS_URL_STATUS_CODE),
        UrlType::DownloadUrl => (dcat::DOWNLOAD_URL, dcat_mqa::DOWNLOAD_URL_STATUS_CODE),
    };
    store.insert(Quad::new(distribution, predicate, url, GraphName::DefaultGraph).as_ref())?;
    add_quality_measurement(metric, distribution, url.into(), status_code, store)
}

/// Add quality measurement to metric store
pub fn add_quality_measurement(
    metric: NamedNodeRef,
    target: NamedOrBlankNodeRef,
    computed_on: NamedOrBlankNodeRef,
    value: u16,
    store: &Store,
) -> Result<BlankNode, Error> {
    let measurement = BlankNode::default();
    let value_term = Term::Literal(Literal::new_typed_literal(
        format!("{}", value),
        xsd::INTEGER,
    ));

    store.insert(
        Quad::new(
            measurement.as_ref(),
            rdf::TYPE,
            dqv::QUALITY_MEASUREMENT_CLASS,
            GraphName::DefaultGraph,
        )
        .as_ref(),
    )?;
    store.insert(
        Quad::new(
            measurement.as_ref(),
            dqv::IS_MEASUREMENT_OF,
            metric,
            GraphName::DefaultGraph,
        )
        .as_ref(),
    )?;
    store.insert(
        Quad::new(
            measurement.as_ref(),
            dqv::COMPUTED_ON,
            computed_on,
            GraphName::DefaultGraph,
        )
        .as_ref(),
    )?;
    store.insert(
        Quad::new(
            measurement.as_ref(),
            dqv::VALUE,
            value_term,
            GraphName::DefaultGraph,
        )
        .as_ref(),
    )?;
    store.insert(
        Quad::new(
            target,
            dqv::HAS_QUALITY_MEASUREMENT,
            measurement.as_ref(),
            GraphName::DefaultGraph,
        )
        .as_ref(),
    )?;

    Ok(measurement)
}

/// Dump graph as turtle string
pub fn dump_graph_as_turtle(store: &Store) -> Result<Vec<u8>, SerializerError> {
    let mut buffer = Vec::new();
    store.dump_graph(&mut buffer, GraphFormat::Turtle, GraphNameRef::DefaultGraph)?;
    Ok(buffer)
}
