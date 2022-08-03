use std::env;

use cached::{proc_macro::cached, Return};
use lazy_static::lazy_static;
use oxigraph::{
    model::{NamedNode, NamedNodeRef, Quad, Term},
    store::Store,
};
use reqwest::blocking::Client;
use sha2::{
    digest::{
        consts::U16,
        generic_array::{sequence::Split, GenericArray},
    },
    Digest, Sha256,
};
use url::Url;
use uuid::Uuid;

use crate::{
    error::Error,
    rdf::{
        add_quality_measurement, dump_graph_as_turtle, extract_urls_from_distribution,
        get_dataset_node, insert_dataset_assessment, insert_distribution_assessment,
        list_distributions, parse_turtle,
    },
    vocab::dcat_mqa,
};

lazy_static! {
    pub static ref MQA_URI_BASE: String =
        env::var("MQA_URI_BASE").unwrap_or("http://localhost:8080".to_string());
}

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

pub fn parse_rdf_graph_and_check_urls(fdk_id: &String, graph: String) -> Result<String, Error> {
    let store = parse_turtle(graph)?;
    let dataset_node = get_dataset_node(&store).ok_or("Dataset node not found in graph")?;
    let metrics_store = check_urls(fdk_id, dataset_node.as_ref(), &store)?;
    let bytes = dump_graph_as_turtle(&metrics_store)?;
    let turtle = std::str::from_utf8(bytes.as_slice())
        .map_err(|e| format!("Failed converting graph to string: {}", e))?;
    Ok(turtle.to_string())
}

fn uuid_from_str(s: String) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(s);
    let hash = hasher.finalize();
    let (head, _): (GenericArray<_, U16>, _) = Split::split(hash);
    uuid::Uuid::from_u128(u128::from_le_bytes(*head.as_ref()))
}

fn check_urls(fdk_id: &String, dataset_node: NamedNodeRef, store: &Store) -> Result<Store, Error> {
    let dataset_assessment = NamedNode::new(format!(
        "{}/assessments/datasets/{}",
        MQA_URI_BASE.clone(),
        fdk_id
    ))?;

    let metrics_store = Store::new()?;
    insert_dataset_assessment(dataset_assessment.as_ref(), dataset_node, &metrics_store)?;

    for dist in list_distributions(dataset_node, store).collect::<Result<Vec<Quad>, _>>()? {
        let distribution = if let Term::NamedNode(node) = dist.object.clone() {
            node
        } else {
            tracing::warn!("Distribution is not a named node {}", fdk_id);
            continue;
        };

        let distribution_assessment = NamedNode::new(format!(
            "{}/assessments/distributions/{}",
            MQA_URI_BASE.clone(),
            uuid_from_str(distribution.as_str().to_string())
        ))?;

        insert_distribution_assessment(
            dataset_assessment.as_ref(),
            distribution_assessment.as_ref(),
            distribution.as_ref(),
            &metrics_store,
        )?;

        tracing::info!("{} - Extracting urls from distribution", fdk_id);
        let urls = extract_urls_from_distribution(distribution.as_ref(), &store)?;
        tracing::info!("{} - Number of urls found {}", fdk_id, urls.len());

        for url in urls {
            let result = check_url(&url);
            tracing::info!("{}", result.note);

            let metric = match url.url_type {
                UrlType::AccessUrl => dcat_mqa::ACCESS_URL_STATUS_CODE,
                UrlType::DownloadUrl => dcat_mqa::DOWNLOAD_URL_STATUS_CODE,
            };
            add_quality_measurement(
                metric,
                distribution_assessment.as_ref(),
                distribution.as_ref(),
                result.status,
                &metrics_store,
            )?;
        }
    }

    Ok(metrics_store)
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

    #[test]
    fn test_parse_graph_anc_collect_metrics() {
        let mqa_graph = parse_rdf_graph_and_check_urls(&"0123bf37-5867-4c90-bc74-5a8c4e118572".to_string(), r#"
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
                dcat:distribution <https://dist.foo> ; 
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

            <https://dist.foo> rdf:type dcat:Distribution ; dct:description "Norsk bistand i tall etter partner"@nb ; 
                dct:format <https://www.iana.org/assignments/media-types/application/vnd.openxmlformats-officedocument.spreadsheetml.sheet> , 
                        <https://www.iana.org/assignments/media-types/text/csv> ; 
                dct:license <http://data.norge.no/nlod/no/2.0> ; 
                dct:title "Bistandsresultater - bistand etter partner"@nb ; 
                dcat:accessURL <http://invalid.url.no> .
        "#.to_string()).unwrap();

        let store_actual = parse_turtle(mqa_graph).unwrap();
        let graph_bytes = dump_graph_as_turtle(&store_actual).unwrap();
        let graph_actual = std::str::from_utf8(&graph_bytes).unwrap();

        assert_eq!(
            sorted_lines(replace_blank(graph_actual)),
            sorted_lines(replace_blank(
                r#"
                    <http://localhost:8080/assessments/datasets/0123bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://data.norge.no/vocabulary/dcatno-mqa#DatasetAssessment> .
                    <http://localhost:8080/assessments/datasets/0123bf37-5867-4c90-bc74-5a8c4e118572> <https://data.norge.no/vocabulary/dcatno-mqa#assessmentOf> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
                    <http://localhost:8080/assessments/datasets/0123bf37-5867-4c90-bc74-5a8c4e118572> <https://data.norge.no/vocabulary/dcatno-mqa#hasDistributionAssessment> <http://localhost:8080/assessments/distributions/25c00e79-422c-214f-40ac-ef8ff6c51e2f> .
                    <http://localhost:8080/assessments/distributions/25c00e79-422c-214f-40ac-ef8ff6c51e2f> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://data.norge.no/vocabulary/dcatno-mqa#DistributionAssessment> .
                    <http://localhost:8080/assessments/distributions/25c00e79-422c-214f-40ac-ef8ff6c51e2f> <https://data.norge.no/vocabulary/dcatno-mqa#assessmentOf> <https://dist.foo> .
                    <http://localhost:8080/assessments/distributions/25c00e79-422c-214f-40ac-ef8ff6c51e2f> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:3b11ced7b58fe980add2ebc6b57941ca .
                    _:3b11ced7b58fe980add2ebc6b57941ca <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
                    _:3b11ced7b58fe980add2ebc6b57941ca <http://www.w3.org/ns/dqv#computedOn> <https://dist.foo> .
                    _:3b11ced7b58fe980add2ebc6b57941ca <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#accessUrlStatusCode> .
                    _:3b11ced7b58fe980add2ebc6b57941ca <http://www.w3.org/ns/dqv#value> "400"^^<http://www.w3.org/2001/XMLSchema#integer> .
                "#
            ))
        )
    }
}
