use log::info;
use schema_registry_converter::blocking::schema_registry::{
    post_schema, 
    SrSettings
};
use schema_registry_converter::schema_registry_common::{
    SuppliedSchema, 
    SchemaType
};
use serde_derive::{Serialize, Deserialize};

#[derive(Debug, Serialize)]
#[allow(non_camel_case_types)]
pub enum MQAEventType {
    URLS_CHECKED
}

#[derive(Debug, Serialize)]
#[allow(non_snake_case)]
pub struct MQAEvent {
    pub r#type: MQAEventType,    
    pub fdkId: String,
    pub graph: String,
    pub timestamp: i64
}

#[derive(Debug, Deserialize)]
#[allow(non_camel_case_types)]
pub enum DatasetEventType {
    DATASET_HARVESTED
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
pub struct DatasetEvent {
    pub r#type: DatasetEventType,
    pub fdkId: String,
    pub graph: String,
    pub timestamp: i64
}

pub fn setup_schemas(sr_settings: &SrSettings) {
    info!("Setting up schemas");

    let schema = SuppliedSchema {
        name: Some(String::from("no.fdk.mqa.MQAEvent")),
        schema_type: SchemaType::Avro,
        schema: String::from(r#"{
            "name": "MQAEvent",
            "namespace": "no.fdk.mqa",
            "type": "record",
            "fields": [
                {
                    "name": "type", 
                    "type": {
                        "type": "enum",
                        "name": "MQAEventType",
                        "symbols": ["URLS_CHECKED", "MODEL_CHECKED", "DCAT_COMPLIANCE_CHECKED", "SCORE_CALCULATED"]
                    }
                },
                {"name": "fdkId", "type": "string"},
                {"name": "graph", "type": "string"},
                {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"}
            ]
        }"#),
        references: vec![],
    };
    
    match post_schema(sr_settings, String::from("no.fdk.mqa.MQAEvent"), schema) {
        Ok(result) => {info!("Schema succesfully registered with id={}", result.id)}
        Err(e) => {panic!("Schema could not be registered {}", e); }
    }
    
}

#[allow(dead_code)]
fn main() {
    println!("This is not an executable");
}





// pub fn get_avro_schemas() -> Result<Vec<Schema>, Error> {
//     let raw_schema_dataset_event = r#"{
//             "name": "DatasetEvent",
//             "type": "record",
//             "fields": [
//                 {
//                     "name": "type", 
//                     "type": {
//                         "type": "enum",
//                         "name": "DatasetEventType",
//                         "symbols": ["DATASET_HARVESTED"]
//                     }
//                 }
//                 {"name": "fdkId", "type": "string"}
//                 {"name": "graph", "type": "string"}
//                 {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"}
//             ]
//         }"#;

//     // This definition depends on the definition of A above
//     let raw_schema_mqa_event = r#"{
//             "name": "MQAEvent",
//             "type": "record",
//             "fields": [
//                 {
//                     "name": "type", 
//                     "type": {
//                         "type": "enum",
//                         "name": "MQAEventType",
//                         "symbols": ["URLS_CHECKED", "MODEL_CHECKED", "DCAT_COMPLIANCE_CHECKED", "SCORE_CALCULATED"]
//                     }
//                 }
//                 {"name": "fdkId", "type": "string"}
//                 {"name": "graph", "type": "string"}
//                 {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"}
//             ]
//         }"#;

//     // if the schemas are not valid, this function will return an error
//     return Schema::parse_list(&[raw_schema_dataset_event, raw_schema_mqa_event]);
// }