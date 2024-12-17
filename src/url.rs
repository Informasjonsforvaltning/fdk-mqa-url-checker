use cached::{proc_macro::cached, Return};
use oxigraph::{
    model::{NamedNodeRef, Quad, Term},
    store::Store,
};
use reqwest::blocking::Client;
use url::Url;

use crate::{
    error::Error,
    rdf::{
        add_quality_measurement, dump_graph_as_turtle, extract_urls_from_distribution,
        get_dataset_node, insert_dataset_assessment, insert_distribution_assessment,
        list_distributions, node_assessment, parse_turtle,
    },
    vocab::dcat_mqa,
};

#[derive(Debug, Clone)]
pub enum UrlType {
    AccessUrl,
    DownloadUrl,
}

#[derive(Debug, Clone)]
pub struct UrlCheck {
    pub method: String,
    pub url_type: UrlType,
    pub url: String,
}
#[derive(Debug, Clone)]
pub struct UrlCheckResult {
    pub url: String,
    pub url_type: UrlType,
    pub status: u16,
    pub note: String,
}

pub fn parse_rdf_graph_and_check_urls(
    input_store: &Store,
    output_store: &Store,
    graph: String,
) -> Result<String, Error> {
    input_store.clear()?;
    output_store.clear()?;
    parse_turtle(input_store, graph)?;
    let dataset_node = get_dataset_node(input_store).ok_or("Dataset node not found in graph")?;
    check_urls(dataset_node.as_ref(), input_store, output_store)?;
    let bytes = dump_graph_as_turtle(output_store)?;
    let turtle = std::str::from_utf8(bytes.as_slice())
        .map_err(|e| format!("Failed converting graph to string: {}", e))?;
    Ok(turtle.to_string())
}

fn check_urls(
    dataset_node: NamedNodeRef,
    input_store: &Store,
    output_store: &Store,
) -> Result<(), Error> {
    let dataset_assessment = node_assessment(input_store, dataset_node)?;

    insert_dataset_assessment(dataset_assessment.as_ref(), dataset_node, &output_store)?;

    for dist in list_distributions(dataset_node, input_store).collect::<Result<Vec<Quad>, _>>()? {
        let distribution = if let Term::NamedNode(node) = dist.object.clone() {
            node
        } else {
            tracing::warn!("distribution is not a named node");
            continue;
        };

        let distribution_assessment = node_assessment(&input_store, distribution.as_ref())?;
        insert_distribution_assessment(
            dataset_assessment.as_ref(),
            distribution_assessment.as_ref(),
            distribution.as_ref(),
            &output_store,
        )?;

        let urls = extract_urls_from_distribution(distribution.as_ref(), input_store)?;
        tracing::debug!(count = urls.len(), "number of urls found");

        for url in urls {
            let result = check_url(&url);
            tracing::debug!(note = result.note, "note");

            let metric = match url.url_type {
                UrlType::AccessUrl => dcat_mqa::ACCESS_URL_STATUS_CODE,
                UrlType::DownloadUrl => dcat_mqa::DOWNLOAD_URL_STATUS_CODE,
            };
            add_quality_measurement(
                metric,
                distribution_assessment.as_ref(),
                distribution.as_ref(),
                result.status,
                &output_store,
            )?;
        }
    }

    Ok(())
}

pub fn check_url(url_check: &UrlCheck) -> UrlCheckResult {
    let parsed_url = Url::parse(url_check.url.as_str());

    match parsed_url {
        Ok(mut u) => {
            u.set_query(None);
            let mut check_result = perform_url_check(
                url_check.method.clone(),
                url_check.url.clone(),
                url_check.url_type.clone(),
                u.to_string(),
            );

            if check_result.was_cached {
                check_result.note = "Cached value".to_string()
            };

            check_result.value
        }
        Err(_) => UrlCheckResult {
            url: url_check.url.clone(),
            url_type: url_check.url_type.clone(),
            status: 400,
            note: "URL is invalid".to_string(),
        },
    }
}

#[cached(
    time = 300,
    with_cached_flag = true,
    key = "String",
    convert = r#"{ format!("{}", _parsed_url) }"#
)]
fn perform_url_check(
    method: String,
    url: String,
    url_type: UrlType,
    _parsed_url: String,
) -> Return<UrlCheckResult> {
    let mut check_result = UrlCheckResult {
        url: url.clone(),
        url_type: url_type.clone(),
        status: 0,
        note: "".to_string(),
    };

    // Create a client so we can make requests
    let client = Client::new();

    let mut final_url = url.clone();
    if method != "HEAD" {
        final_url = get_geo_url(method.clone(), final_url);
    }

    match client
        .request(
            http::Method::from_bytes(method.as_bytes()).unwrap_or(http::Method::GET),
            final_url.as_str(),
        )
        .send()
    {
        Ok(resp) => {
            check_result.note = "Response value".to_string();
            check_result.status = resp.status().as_u16();

            if check_result.status == 405 {
                return perform_url_check("GET".to_string(), url, url_type, _parsed_url);
            }
        }
        Err(e) => {
            check_result.note = e.to_string();
            check_result.status = 400;
        }
    }

    Return::new(check_result)
}

fn get_geo_url(method: String, url: String) -> String {
    let parsed_url = Url::parse(url.as_str());

    match parsed_url {
        Ok(mut u) => {
            if !u.query().unwrap_or("").contains("request=GetCapabilities")
                && !u.query().unwrap_or("").contains("REQUEST=GetCapabilities")
            {
                u.set_query(Some(
                    format!("request=GetCapabilities&service={}", method).as_str(),
                ));
            }
            u.to_string()
        }
        Err(e) => {
            tracing::warn!("Parsing geo URL failed {}", e);
            url.to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sophia_api::term::SimpleTerm;
    use sophia_api::source::TripleSource;
    use sophia_isomorphism::isomorphic_graphs;
    use sophia_turtle::parser::turtle::parse_str;

    #[test]
    fn test_parse_graph_anc_collect_metrics() {
        let mqa_graph = parse_rdf_graph_and_check_urls(
            &mut Store::new().unwrap(),
            &mut Store::new().unwrap(),
            include_str!("../tests/data/dataset_event.ttl").to_string(),
        )
        .unwrap();

        let result_graph: Vec<[SimpleTerm; 3]> = parse_str(&mqa_graph.as_str())
            .collect_triples()
            .unwrap();
        let expected_graph: Vec<[SimpleTerm; 3]> = parse_str(include_str!("../tests/data/mqa_event.ttl"))
            .collect_triples()
            .unwrap();

        assert!(isomorphic_graphs(&expected_graph, &result_graph).unwrap())
    }
}
