use chrono::Utc;
use log::{info, warn};

use oxigraph::{
    model::{GraphName, NamedNodeRef, NamedOrBlankNode, Quad, Term},
    store::{StorageError, Store},
};
use url::Url;

use reqwest::blocking::Client;

use cached::proc_macro::cached;
use cached::Return;

use serde_derive::Serialize;

use crate::{
    rdf::{
        add_access_url_status_metric, add_download_url_status_metric, create_metrics_store,
        dump_graph_as_turtle, extract_urls_from_distribution, get_dataset_node, list_distributions,
        parse_turtle,
    },
    schemas::{MQAEvent, MQAEventType},
};

#[derive(Debug, Clone, Serialize)]
pub enum UrlType {
    #[serde(rename = "ACCESS_URL")]
    AccessUrl,
    #[serde(rename = "DOWNLOAD_URL")]
    DownloadUrl,
}

#[derive(Debug, Clone)]
pub struct UrlCheckResult {
    pub url: String,
    pub url_type: UrlType,
    pub status: u16,
    pub note: String,
}

pub fn parse_rdf_graph_and_check_urls(
    fdk_id: String,
    graph: String,
) -> Result<Option<MQAEvent>, String> {
    match parse_turtle(graph) {
        Ok(store) => {
            match get_dataset_node(&store) {
                Some(dataset_node) => {
                    match check_urls(fdk_id.clone(), dataset_node.as_ref(), &store) {
                        Ok(metrics_store) => {
                            match dump_graph_as_turtle(&metrics_store) {
                                Ok(bytes) => {
                                    // Create MQA event
                                    match std::str::from_utf8(bytes.as_slice()) {
                                        Ok(turtle) => Ok(Some(MQAEvent {
                                            event_type: MQAEventType::UrlsChecked,
                                            fdk_id: fdk_id.clone(),
                                            graph: turtle.to_string(),
                                            timestamp: Utc::now().timestamp_millis(),
                                        })),
                                        Err(e) => Err(format!(
                                            "{} - Failed dumping graph as turtle: {}",
                                            fdk_id, e
                                        )),
                                    }
                                }
                                Err(e) => Err(format!(
                                    "{} - Failed dumping graph as turtle: {}",
                                    fdk_id, e
                                )),
                            }
                        }
                        Err(e) => Err(format!(
                            "{} - Failed creating new metrics store: {}",
                            fdk_id, e
                        )),
                    }
                }
                None => Err(format!("{} - Dataset node not found in graph", fdk_id)),
            }
        }
        Err(e) => Err(format!("{} - Failed to parse graph: {}", fdk_id, e)),
    }
}

fn check_urls(
    fdk_id: String,
    dataset_node: NamedNodeRef,
    store: &Store,
) -> Result<Store, StorageError> {
    // Make MQA metrics model (DQV)
    let metrics_store = create_metrics_store(dataset_node)?;
    let distributions =
        list_distributions(dataset_node, store).collect::<Result<Vec<Quad>, _>>()?;

    for dist in distributions {
        metrics_store.insert(
            Quad::new(
                dist.subject.clone(),
                dist.predicate.clone(),
                dist.object.clone(),
                GraphName::DefaultGraph,
            )
            .as_ref(),
        )?;

        let dist_node = match dist.object {
            Term::NamedNode(n) => Some(NamedOrBlankNode::NamedNode(n)),
            Term::BlankNode(n) => Some(NamedOrBlankNode::BlankNode(n)),
            _ => None,
        }
        .unwrap();

        info!("{} - Extracting urls from distribution", fdk_id);
        let urls = extract_urls_from_distribution(dist_node.as_ref(), &store);
        info!("{} - Number of urls found {}", fdk_id, urls.len());

        for url in urls {
            let result = check_url(
                url.get("method").unwrap().to_string(),
                if url.contains_key("access_url") {
                    url.get("access_url").unwrap().to_string()
                } else {
                    url.get("download_url").unwrap().to_string()
                },
                if url.contains_key("access_url") {
                    UrlType::AccessUrl
                } else {
                    UrlType::DownloadUrl
                },
            );

            info!("{}", result.note);

            let url_node: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(result.url.as_str());

            match result.url_type {
                UrlType::AccessUrl => add_access_url_status_metric(
                    dist_node.as_ref(),
                    url_node,
                    result.status,
                    &metrics_store,
                )?,
                UrlType::DownloadUrl => add_download_url_status_metric(
                    dist_node.as_ref(),
                    url_node,
                    result.status,
                    &metrics_store,
                )?,
            };
        }
    }

    Ok(metrics_store)
}

pub fn check_url(method: String, url: String, url_type: UrlType) -> UrlCheckResult {
    let parsed_url = Url::parse(url.as_str());

    match parsed_url {
        Ok(mut u) => {
            u.set_query(None);
            let mut check_result = perform_url_check(method, url, url_type, u.to_string());

            if check_result.was_cached {
                check_result.note = "Cached value".to_string()
            };

            check_result.value
        }
        Err(_) => UrlCheckResult {
            url: url,
            url_type: url_type,
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
            http::Method::from_bytes(method.as_bytes()).unwrap(),
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
            if !u.query().unwrap().contains("request=GetCapabilities")
                && !u.query().unwrap().contains("REQUEST=GetCapabilities")
            {
                u.set_query(Some(
                    format!("request=GetCapabilities&service={}", method).as_str(),
                ));
            }
            u.to_string()
        }
        Err(e) => {
            warn!("Parsing geo URL failed {}", e);
            url.to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use oxigraph::model::{NamedOrBlankNodeRef, TermRef};

    use super::*;
    use crate::utils::setup_logger;
    use crate::vocab::dqv;

    fn convert_term_to_named_or_blank_node_ref(term: TermRef) -> Option<NamedOrBlankNodeRef> {
        match term {
            TermRef::NamedNode(node) => Some(NamedOrBlankNodeRef::NamedNode(node)),
            TermRef::BlankNode(node) => Some(NamedOrBlankNodeRef::BlankNode(node)),
            _ => None,
        }
    }

    #[test]
    fn test_parse_graph_anc_collect_metrics() {
        setup_logger(true, None);

        let mqa_event_result = parse_rdf_graph_and_check_urls("1".to_string(), r#"
            @prefix adms: <http://www.w3.org/ns/adms#> . 
            @prefix cpsv: <http://purl.org/vocab/cpsv#> . 
            @prefix cpsvno: <https://data.norge.no/vocabulary/cpsvno#> . 
            @prefix dcat: <http://www.w3.org/ns/dcat#> . 
            @prefix dct: <http://purl.org/dc/terms/> . 
            @prefix dqv: <http://www.w3.org/ns/dqv#> . 
            @prefix eli: <http://data.europa.eu/eli/ontology#> . 
            @prefix foaf: <http://xmlns.com/foaf/0.1/> . 
            @prefix iso: <http://iso.org/25012/2008/dataquality/> . 
            @prefix oa: <http://www.w3.org/ns/oa#> . 
            @prefix prov: <http://www.w3.org/ns/prov#> . 
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> . 
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . 
            @prefix schema: <http://schema.org/> . 
            @prefix skos: <http://www.w3.org/2004/02/skos/core#> . 
            @prefix vcard: <http://www.w3.org/2006/vcard/ns#> . 
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . 
            
            <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> rdf:type dcat:Dataset ; 
                dct:accessRights <http://publications.europa.eu/resource/authority/access-right/PUBLIC> ; 
                dct:description "Visning over all norsk offentlig bistand fra 1960 til siste kalenderår sortert etter partnerorganisasjoner."@nb ; 
                dct:identifier "https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572" ; 
                dct:language <http://publications.europa.eu/resource/authority/language/NOR> , <http://publications.europa.eu/resource/authority/language/ENG> ; 
                dct:provenance <http://data.brreg.no/datakatalog/provinens/nasjonal> ; 
                dct:publisher <https://organization-catalogue.fellesdatakatalog.digdir.no/organizations/971277882> ; 
                dct:title "Bistandsresultater - bistand etter partner"@nb ; 
                dct:type "Data" ; 
                dcat:contactPoint [ rdf:type vcard:Organization ; vcard:hasEmail <mailto:resultater@norad.no> ] ; 
                dcat:distribution [ 
                    rdf:type dcat:Distribution ; dct:description "Norsk bistand i tall etter partner"@nb ; 
                    dct:format <https://www.iana.org/assignments/media-types/application/vnd.openxmlformats-officedocument.spreadsheetml.sheet> , 
                            <https://www.iana.org/assignments/media-types/text/csv> ; 
                    dct:license <http://data.norge.no/nlod/no/2.0> ; 
                    dct:title "Bistandsresultater - bistand etter partner"@nb ; 
                    dcat:accessURL <http://invalid.url.no> ] ; 
                dcat:keyword "oda"@nb , "norad"@nb , "bistand"@nb ; 
                dcat:landingPage <http://resultater.norad.no/partner/> ; 
                dcat:theme <http://publications.europa.eu/resource/authority/data-theme/INTR> ; 
                dqv:hasQualityAnnotation [ rdf:type dqv:QualityAnnotation ; dqv:inDimension iso:Currentness ] ; 
                prov:qualifiedAttribution [ 
                    rdf:type prov:Attribution ; 
                    dcat:hadRole <http://registry.it.csiro.au/def/isotc211/CI_RoleCode/contributor> ; 
                    prov:agent <https://data.brreg.no/enhetsregisteret/api/enheter/971277882> ] . 
                <http://publications.europa.eu/resource/authority/language/ENG> rdf:type dct:LinguisticSystem ; 
                    <http://publications.europa.eu/ontology/authority/authority-code> "ENG" ; 
                    skos:prefLabel "Engelsk"@nb . 
                <http://publications.europa.eu/resource/authority/language/NOR> rdf:type dct:LinguisticSystem ; 
                    <http://publications.europa.eu/ontology/authority/authority-code> "NOR" ; skos:prefLabel "Norsk"@nb .
        "#.to_string());

        assert!(mqa_event_result.is_ok());
        let mqa_event_option = mqa_event_result.unwrap();
        assert!(mqa_event_option.is_some());
        let store_actual_result = parse_turtle(mqa_event_option.unwrap().graph);
        assert!(store_actual_result.is_ok());
        let store_actual = store_actual_result.unwrap();
        assert_eq!(
            8,
            store_actual
                .quads_for_pattern(None, None, None, None)
                .count()
        );

        let dataset = NamedNodeRef::new_unchecked("https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572");
        let mut count = 0;

        for dr in list_distributions(dataset, &store_actual) {
            count = count + 1;

            if let Ok(dist_quad) = dr {
                let dist = convert_term_to_named_or_blank_node_ref(dist_quad.object.as_ref());
                assert!(dist.is_some());

                let mr = store_actual
                    .quads_for_pattern(
                        dist.map(|d| d.into()),
                        Some(dqv::HAS_QUALITY_MEASUREMENT),
                        None,
                        None,
                    )
                    .next()
                    .unwrap();

                if let Ok(measurement_quad) = mr {
                    let measurement =
                        convert_term_to_named_or_blank_node_ref(measurement_quad.object.as_ref());

                    let vr = store_actual
                        .quads_for_pattern(
                            measurement.map(|m| m.into()),
                            Some(dqv::VALUE),
                            None,
                            None,
                        )
                        .next()
                        .unwrap();

                    if let Ok(value_quad) = vr {
                        info!("{}", value_quad);
                        match value_quad.object {
                            Term::Literal(value) => assert_eq!("400", value.value()),
                            _ => assert!(false),
                        }
                    } else {
                        assert!(false);
                    }
                } else {
                    assert!(false);
                }
            } else {
                assert!(false);
            }
        }
        assert_eq!(1, count);
    }
}
