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
        let mqa_graph = parse_rdf_graph_and_check_urls(&mut Store::new().unwrap(), &mut Store::new().unwrap(), r#"
            @prefix adms: <http://www.w3.org/ns/adms#> . 
            @prefix cpsv: <http://purl.org/vocab/cpsv#> . 
            @prefix cpsvno: <https://data.norge.no/vocabulary/cpsvno#> . 
            @prefix dcat: <http://www.w3.org/ns/dcat#> . 
            @prefix dcatnomqa: <https://data.norge.no/vocabulary/dcatno-mqa#> . 
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
                dcatnomqa:hasAssessment <http://dataset.assessment.no> ;
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
                dcatnomqa:hasAssessment <http://dist.foo.assessment.no> ;
                dct:format <https://www.iana.org/assignments/media-types/application/vnd.openxmlformats-officedocument.spreadsheetml.sheet> , 
                        <https://www.iana.org/assignments/media-types/text/csv> ; 
                dct:license <http://data.norge.no/nlod/no/2.0> ; 
                dct:title "Bistandsresultater - bistand etter partner"@nb ; 
                dcat:accessURL <http://invalid.url.no> .
        "#.to_string()).unwrap();

        assert_eq!(
            sorted_lines(replace_blank(mqa_graph.as_str())),
            sorted_lines(replace_blank(
                r#"
                    <http://dataset.assessment.no> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://data.norge.no/vocabulary/dcatno-mqa#DatasetAssessment> .
                    <http://dataset.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#assessmentOf> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
                    <http://dataset.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#hasDistributionAssessment> <http://dist.foo.assessment.no> .
                    <http://dist.foo.assessment.no> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://data.norge.no/vocabulary/dcatno-mqa#DistributionAssessment> .
                    <http://dist.foo.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#assessmentOf> <https://dist.foo> .
                    <http://dist.foo.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:3b11ced7b58fe980add2ebc6b57941ca .
                    _:3b11ced7b58fe980add2ebc6b57941ca <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
                    _:3b11ced7b58fe980add2ebc6b57941ca <http://www.w3.org/ns/dqv#computedOn> <https://dist.foo> .
                    _:3b11ced7b58fe980add2ebc6b57941ca <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#accessUrlStatusCode> .
                    _:3b11ced7b58fe980add2ebc6b57941ca <http://www.w3.org/ns/dqv#value> "400"^^<http://www.w3.org/2001/XMLSchema#integer> .
                "#
            ))
        )
    }
}
