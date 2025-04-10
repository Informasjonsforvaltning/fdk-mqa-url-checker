use oxigraph::{
    io::{RdfFormat, RdfParser},
    model::{
        vocab::{rdf, xsd},
        BlankNode, GraphName, GraphNameRef, Literal, NamedNode, NamedNodeRef, Quad, Subject, Term,
    },
    store::{QuadIter, SerializerError, Store},
};

use crate::{
    error::Error,
    url::{UrlCheck, UrlType},
    vocab::{dcat, dcat_mqa, dcterms, dqv},
};

/// Parse Turtle RDF and load into store
pub fn parse_turtle(store: &Store, turtle: String) -> Result<(), Error> {
    store.load_from_reader(
        RdfParser::from_format(RdfFormat::Turtle)
            .without_named_graphs()
            .with_default_graph(GraphNameRef::DefaultGraph),
        turtle.to_string().as_bytes().as_ref()
    )?;
    Ok(())
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
pub fn list_access_urls(distribution: NamedNodeRef, store: &Store) -> QuadIter {
    store.quads_for_pattern(
        Some(distribution.into()),
        Some(dcat::ACCESS_URL.into()),
        None,
        None,
    )
}

/// Retrieve download urls of a distribution
pub fn list_download_urls(distribution: NamedNodeRef, store: &Store) -> QuadIter {
    store.quads_for_pattern(
        Some(distribution.into()),
        Some(dcat::DOWNLOAD_URL.into()),
        None,
        None,
    )
}

/// Retrieve distribution formats
pub fn list_formats(distribution: NamedNodeRef, store: &Store) -> QuadIter {
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

/// Extract assessment of node.
pub fn node_assessment(store: &Store, node: NamedNodeRef) -> Result<NamedNode, Error> {
    store
        .quads_for_pattern(
            Some(node.into()),
            Some(dcat_mqa::HAS_ASSESSMENT.into()),
            None,
            None,
        )
        .next()
        .ok_or(Error::from(format!(
            "assessment not found for node '{}'",
            node,
        )))?
        .map(|d| match d {
            Quad {
                object: Term::NamedNode(n),
                ..
            } => Ok(n),
            _ => Err(format!(
                "assessment of node '{}' is not a named node: '{}'",
                node, d.object
            )
            .into()),
        })?
}

/// Extract accessURLs and downloadURLs from dataset.
pub fn extract_urls_from_distribution(
    distribution: NamedNodeRef,
    store: &Store,
) -> Result<Vec<UrlCheck>, Error> {
    let mut urls = Vec::new();

    // Map format to HEAD
    let head = match list_formats(distribution, store).next() {
        Some(fmt) => map_format_to_head(fmt?.object.to_string()),
        None => "HEAD".to_string(),
    };

    for acc_url_result in list_access_urls(distribution, store) {
        match acc_url_result?.object {
            Term::NamedNode(acc_url_node) => {
                urls.push(UrlCheck {
                    method: head.to_string(),
                    url: acc_url_node.into_string(),
                    url_type: UrlType::AccessUrl,
                });
            }
            node => tracing::warn!(
                node = node.to_string(),
                "access URL node is not a NamedNode but"
            ),
        }
    }

    for dl_url_result in list_download_urls(distribution, store) {
        match dl_url_result?.object {
            Term::NamedNode(dl_url_node) => {
                urls.push(UrlCheck {
                    method: head.to_string(),
                    url: dl_url_node.into_string(),
                    url_type: UrlType::DownloadUrl,
                });
            }
            node => tracing::warn!(
                node = node.to_string(),
                "download URL node is not a NamedNode"
            ),
        }
    }

    Ok(urls)
}

/// Insert dataset assessment into store
pub fn insert_dataset_assessment(
    dataset_assessment: NamedNodeRef,
    dataset: NamedNodeRef,
    store: &Store,
) -> Result<(), Error> {
    store.insert(&Quad::new(
        dataset_assessment.clone(),
        rdf::TYPE,
        dcat_mqa::DATASET_ASSESSMENT_CLASS,
        GraphName::DefaultGraph,
    ))?;
    store.insert(&Quad::new(
        dataset_assessment.clone(),
        dcat_mqa::ASSESSMENT_OF,
        dataset,
        GraphName::DefaultGraph,
    ))?;

    Ok(())
}

/// Insert distribution assessment into store.
pub fn insert_distribution_assessment(
    dataset_assessment: NamedNodeRef,
    distribution_assessment: NamedNodeRef,
    distribution: NamedNodeRef,
    store: &Store,
) -> Result<(), Error> {
    store.insert(&Quad::new(
        distribution_assessment,
        rdf::TYPE,
        dcat_mqa::DISTRIBUTION_ASSESSMENT_CLASS,
        GraphName::DefaultGraph,
    ))?;
    store.insert(&Quad::new(
        distribution_assessment.clone(),
        dcat_mqa::ASSESSMENT_OF,
        distribution,
        GraphName::DefaultGraph,
    ))?;
    store.insert(&Quad::new(
        dataset_assessment,
        dcat_mqa::HAS_DISTRIBUTION_ASSESSMENT,
        distribution_assessment,
        GraphName::DefaultGraph,
    ))?;

    Ok(())
}

/// Add quality measurement to metric store
pub fn add_quality_measurement(
    metric: NamedNodeRef,
    target: NamedNodeRef,
    computed_on: NamedNodeRef,
    value: u16,
    store: &Store,
) -> Result<BlankNode, Error> {
    let measurement = BlankNode::default();
    let value_term = Term::Literal(Literal::new_typed_literal(
        format!("{}", value),
        xsd::INTEGER,
    ));

    store.insert(&Quad::new(
        measurement.as_ref(),
        rdf::TYPE,
        dqv::QUALITY_MEASUREMENT_CLASS,
        GraphName::DefaultGraph,
    ))?;
    store.insert(&Quad::new(
        measurement.as_ref(),
        dqv::IS_MEASUREMENT_OF,
        metric,
        GraphName::DefaultGraph,
    ))?;
    store.insert(&Quad::new(
        measurement.as_ref(),
        dqv::COMPUTED_ON,
        computed_on,
        GraphName::DefaultGraph,
    ))?;
    store.insert(&Quad::new(
        measurement.as_ref(),
        dqv::VALUE,
        value_term,
        GraphName::DefaultGraph,
    ))?;
    store.insert(&Quad::new(
        target,
        dcat_mqa::CONTAINS_QUALITY_MEASUREMENT,
        measurement.as_ref(),
        GraphName::DefaultGraph,
    ))?;

    Ok(measurement)
}

/// Dump graph as turtle string
pub fn dump_graph_as_turtle(store: &Store) -> Result<Vec<u8>, SerializerError> {
    let mut buffer = Vec::new();
    store.dump_graph_to_writer(GraphNameRef::DefaultGraph, RdfFormat::Turtle, &mut buffer)?;
    Ok(buffer)
}
