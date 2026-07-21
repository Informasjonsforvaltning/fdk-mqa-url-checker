use cached::{proc_macro::cached, Return};
use lazy_static::lazy_static;
use oxigraph::{
    model::{NamedNodeRef, Quad, Term},
    store::Store,
};
use reqwest::Client;
use std::time::Duration;
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

const URL_CHECK_TIMEOUT_SECS: u64 = 10;
/// Cache TTL in seconds. Must match the `time` value on `perform_url_check`.
const URL_CACHE_TTL_SECS: u64 = 300;
const HTTP_STATUS_METHOD_NOT_ALLOWED: u16 = 405;
const HTTP_STATUS_BAD_REQUEST: u16 = 400;

// `#[cached(time = ...)]` requires a literal; keep it equal to URL_CACHE_TTL_SECS.
const _: () = assert!(URL_CACHE_TTL_SECS == 300);

lazy_static! {
    static ref HTTP_CLIENT: Result<Client, reqwest::Error> = Client::builder()
        .timeout(Duration::from_secs(URL_CHECK_TIMEOUT_SECS))
        .build();
}

#[derive(Debug, Clone)]
pub enum UrlType {
    AccessUrl,
    DownloadUrl,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OgcService {
    Wms,
    Wfs,
    Wcs,
}

impl OgcService {
    fn as_str(&self) -> &'static str {
        match self {
            OgcService::Wms => "WMS",
            OgcService::Wfs => "WFS",
            OgcService::Wcs => "WCS",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum UrlRequestStrategy {
    HttpHead,
    HttpGet,
    OgcGetCapabilities(OgcService),
}

impl UrlRequestStrategy {
    fn http_method(&self) -> http::Method {
        match self {
            UrlRequestStrategy::HttpHead => http::Method::HEAD,
            UrlRequestStrategy::HttpGet | UrlRequestStrategy::OgcGetCapabilities(_) => {
                http::Method::GET
            }
        }
    }

    fn ogc_service(&self) -> Option<&OgcService> {
        match self {
            UrlRequestStrategy::OgcGetCapabilities(service) => Some(service),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct UrlCheck {
    pub strategy: UrlRequestStrategy,
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

pub async fn parse_rdf_graph_and_check_urls(
    input_store: &Store,
    output_store: &Store,
    graph: String,
) -> Result<String, Error> {
    parse_turtle(input_store, graph)?;
    let dataset_node = get_dataset_node(input_store).ok_or("Dataset node not found in graph")?;
    check_urls(dataset_node.as_ref(), input_store, output_store).await?;
    let bytes = dump_graph_as_turtle(output_store)?;
    let turtle = std::str::from_utf8(bytes.as_slice())
        .map_err(|e| format!("Failed converting graph to string: {}", e))?;
    Ok(turtle.to_string())
}

async fn check_urls(
    dataset_node: NamedNodeRef<'_>,
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
            let result = check_url(&url).await;
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

/// Map distribution format URI to the HTTP request strategy used for URL checks.
pub fn format_uri_to_request_strategy(format_uri: String) -> UrlRequestStrategy {
    let fmt = format_uri.split("/").last().unwrap_or_default();
    match fmt {
        "WMS_SRVC" => UrlRequestStrategy::OgcGetCapabilities(OgcService::Wms),
        "WFS_SRVC" => UrlRequestStrategy::OgcGetCapabilities(OgcService::Wfs),
        "WCS_SRVC" => UrlRequestStrategy::OgcGetCapabilities(OgcService::Wcs),
        _ => UrlRequestStrategy::HttpHead,
    }
}

pub async fn check_url(url_check: &UrlCheck) -> UrlCheckResult {
    let parsed_url = Url::parse(url_check.url.as_str());

    match parsed_url {
        Ok(mut u) => {
            u.set_query(None);
            let mut check_result = perform_url_check(
                url_check.strategy.clone(),
                url_check.url.clone(),
                url_check.url_type.clone(),
                u.to_string(),
            ).await;

            if check_result.was_cached {
                check_result.note = "Cached value".to_string()
            };

            check_result.value
        }
        Err(_) => UrlCheckResult {
            url: url_check.url.clone(),
            url_type: url_check.url_type.clone(),
            status: HTTP_STATUS_BAD_REQUEST,
            note: "URL is invalid".to_string(),
        },
    }
}

#[cached(
    // Keep in sync with URL_CACHE_TTL_SECS (proc-macro requires a literal).
    time = 300,
    with_cached_flag = true,
    key = "String",
    convert = r#"{ format!("{:?}:{}", strategy, _parsed_url) }"#
)]
async fn perform_url_check(
    strategy: UrlRequestStrategy,
    url: String,
    url_type: UrlType,
    _parsed_url: String,
) -> Return<UrlCheckResult> {
    let check_result = fetch_url_status(&strategy, &url, &url_type).await;

    if check_result.status == HTTP_STATUS_METHOD_NOT_ALLOWED {
        if let Some(fallback) = method_not_allowed_fallback(&strategy) {
            return Box::pin(perform_url_check(
                fallback,
                url,
                url_type,
                _parsed_url,
            ))
            .await;
        }
    }

    Return::new(check_result)
}

async fn fetch_url_status(
    strategy: &UrlRequestStrategy,
    url: &str,
    url_type: &UrlType,
) -> UrlCheckResult {
    let client = match HTTP_CLIENT.as_ref() {
        Ok(client) => client,
        Err(e) => {
            tracing::error!(error = e.to_string(), "failed to create HTTP client");
            return UrlCheckResult {
                url: url.to_string(),
                url_type: url_type.clone(),
                status: HTTP_STATUS_BAD_REQUEST,
                note: format!("Failed to create HTTP client: {}", e),
            };
        }
    };

    let request_url = build_request_url(strategy, url);

    match execute_http_check(client, strategy, &request_url).await {
        Ok(status) => UrlCheckResult {
            url: url.to_string(),
            url_type: url_type.clone(),
            status,
            note: "Response value".to_string(),
        },
        Err(e) => UrlCheckResult {
            url: url.to_string(),
            url_type: url_type.clone(),
            status: HTTP_STATUS_BAD_REQUEST,
            note: e.to_string(),
        },
    }
}

fn build_request_url(strategy: &UrlRequestStrategy, url: &str) -> String {
    match strategy.ogc_service() {
        Some(service) => append_ogc_get_capabilities_query(service, url),
        None => url.to_string(),
    }
}

async fn execute_http_check(
    client: &Client,
    strategy: &UrlRequestStrategy,
    url: &str,
) -> Result<u16, reqwest::Error> {
    let response = client
        .request(strategy.http_method(), url)
        .send()
        .await?;
    Ok(response.status().as_u16())
}

fn method_not_allowed_fallback(strategy: &UrlRequestStrategy) -> Option<UrlRequestStrategy> {
    match strategy {
        UrlRequestStrategy::HttpHead => Some(UrlRequestStrategy::HttpGet),
        UrlRequestStrategy::HttpGet | UrlRequestStrategy::OgcGetCapabilities(_) => None,
    }
}

fn append_ogc_get_capabilities_query(service: &OgcService, url: &str) -> String {
    match Url::parse(url) {
        Ok(mut u) => {
            if !u.query().unwrap_or("").contains("request=GetCapabilities")
                && !u.query().unwrap_or("").contains("REQUEST=GetCapabilities")
            {
                u.set_query(Some(
                    format!("request=GetCapabilities&service={}", service.as_str()).as_str(),
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
    use tokio::runtime::Runtime;

    #[test]
    fn test_parse_graph_anc_collect_metrics() {
        let mqa_graph = Runtime::new().unwrap().block_on(
            parse_rdf_graph_and_check_urls(
                &mut Store::new().unwrap(),
                &mut Store::new().unwrap(),
                include_str!("../tests/data/dataset_event.ttl").to_string(),
            )
        ).unwrap();

        let result_graph: Vec<[SimpleTerm; 3]> = parse_str(&mqa_graph.as_str())
            .collect_triples()
            .unwrap();
        let expected_graph: Vec<[SimpleTerm; 3]> = parse_str(include_str!("../tests/data/mqa_event.ttl"))
            .collect_triples()
            .unwrap();

        assert!(isomorphic_graphs(&expected_graph, &result_graph).unwrap())
    }
}
