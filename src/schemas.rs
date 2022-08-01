use log::info;
use schema_registry_converter::{
    async_impl::schema_registry::{post_schema, SrSettings},
    schema_registry_common::{SchemaType, SuppliedSchema},
};
use serde_derive::{Deserialize, Serialize};

use crate::error::Error;

#[derive(Debug, Serialize)]
pub enum MQAEventType {
    #[serde(rename = "URLS_CHECKED")]
    UrlsChecked,
}

#[derive(Debug, Serialize)]
pub struct MQAEvent {
    #[serde(rename = "type")]
    pub event_type: MQAEventType,
    #[serde(rename = "fdkId")]
    pub fdk_id: String,
    pub graph: String,
    pub timestamp: i64,
}

#[derive(Eq, PartialEq, Debug, Deserialize)]
pub enum DatasetEventType {
    #[serde(rename = "DATASET_HARVESTED")]
    DatasetHarvested,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
pub struct DatasetEvent {
    #[serde(rename = "type")]
    pub event_type: DatasetEventType,
    #[serde(rename = "fdkId")]
    pub fdk_id: String,
    pub graph: String,
    pub timestamp: i64,
}

pub async fn setup_schemas(sr_settings: &SrSettings) -> Result<u32, Error> {
    let schema = SuppliedSchema {
        name: Some("no.fdk.mqa.MQAEvent".to_string()),
        schema_type: SchemaType::Avro,
        schema: r#"{
                "name": "MQAEvent",
                "namespace": "no.fdk.mqa",
                "type": "record",
                "fields": [
                    {
                        "name": "type", 
                        "type": {
                            "type": "enum",
                            "name": "MQAEventType",
                            "symbols": [
                                "URLS_CHECKED", 
                                "PROPERTIES_CHECKED", 
                                "DCAT_COMPLIANCE_CHECKED", 
                                "SCORE_CALCULATED"
                            ]
                        }
                    },
                    {"name": "fdkId", "type": "string"},
                    {"name": "graph", "type": "string"},
                    {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"}
                ]
            }"#
        .to_string(),
        references: vec![],
    };

    info!("Setting up schemas");
    let result = post_schema(sr_settings, String::from("no.fdk.mqa.MQAEvent"), schema).await?;
    Ok(result.id)
}
