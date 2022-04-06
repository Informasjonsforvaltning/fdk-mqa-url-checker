use std::string;

use oxigraph::{model, store};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    LoaderError(#[from] store::LoaderError),
    #[error(transparent)]
    StorageError(#[from] store::StorageError),
    #[error(transparent)]
    SerializerError(#[from] store::SerializerError),
    #[error(transparent)]
    IriParseError(#[from] model::IriParseError),
    #[error(transparent)]
    FromUtf8Error(#[from] string::FromUtf8Error),
    #[error(transparent)]
    KafkaError(#[from] rdkafka::error::KafkaError),
    #[error(transparent)]
    SRCError(#[from] schema_registry_converter::error::SRCError),
    #[error("{0}")]
    String(String),
}

impl From<&str> for Error {
    fn from(e: &str) -> Self {
        Self::String(e.to_string())
    }
}

impl From<String> for Error {
    fn from(e: String) -> Self {
        Self::String(e)
    }
}
