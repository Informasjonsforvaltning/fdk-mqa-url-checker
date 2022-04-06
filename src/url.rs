use log::{warn};

use url::{Url};

use reqwest::blocking::{Client};

use cached::proc_macro::cached;
use cached::Return;

#[derive(Debug, Clone)]
#[allow(non_camel_case_types)]
pub enum UrlType {
    ACCESS_URL,
    DOWNLOAD_URL
}

#[derive(Debug, Clone)]
#[allow(non_snake_case)]
pub struct UrlCheckResult {
    pub url: String,
    pub url_type: UrlType,
    pub status: u16,
    pub note: String
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
        },
        Err(_) => UrlCheckResult {
            url: url,
            url_type: url_type,
            status: 0,
            note: "URL is invalid".to_string()
        }   
    }    
}

#[cached(
    time=300, 
    with_cached_flag = true,
    key = "String", 
    convert = r#"{ format!("{}", _parsed_url) }"#
)]
fn perform_url_check(method: String, url: String, url_type: UrlType, _parsed_url: String) -> Return<UrlCheckResult> {    
    let mut check_result = UrlCheckResult {
        url: url.clone(),
        url_type: url_type.clone(),
        status: 0,
        note: "".to_string()
    };

    // Create a client so we can make requests
    let client = Client::new();

    let mut final_url = url.clone();
    if method != "HEAD" {
        final_url = get_geo_url(method.clone(), final_url);
    }

    match client.request(http::Method::from_bytes(method.as_bytes()).unwrap(), final_url.as_str()).send() {
        Ok(resp) => {
            check_result.note = "Response value".to_string();
            check_result.status = resp.status().as_u16();

            if check_result.status == 405 {
                return perform_url_check("GET".to_string(), url, url_type, _parsed_url)
            }                

        },
        Err(e) => check_result.note = e.to_string()
    }
        
    Return::new(check_result)
}

fn get_geo_url(method: String, url: String) -> String {
    let parsed_url = Url::parse(url.as_str());

    match parsed_url {
        Ok(mut u) => {
            if  !u.query().unwrap().contains("request=GetCapabilities") && 
                !u.query().unwrap().contains("REQUEST=GetCapabilities") {
                    u.set_query(Some(format!("request=GetCapabilities&service={}", method).as_str()));                    
            }
            u.to_string()
        },
        Err(e) => {
            warn!("Parsing geo URL failed {}", e);
            url.to_string()
        }
    }
}

#[allow(dead_code)]
fn main() {
    println!("This is not an executable");
}