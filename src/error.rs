#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    LoaderError(#[from] oxigraph::store::LoaderError),
    #[error(transparent)]
    StorageError(#[from] oxigraph::store::StorageError),
    #[error(transparent)]
    SerializerError(#[from] oxigraph::store::SerializerError),
    #[error(transparent)]
    IriParseError(#[from] oxigraph::model::IriParseError),
    #[error(transparent)]
    FromUtf8Error(#[from] std::string::FromUtf8Error),
    #[error(transparent)]
    KafkaError(#[from] rdkafka::error::KafkaError),
    #[error(transparent)]
    AvroError(#[from] apache_avro::Error),
    #[error(transparent)]
    SRCError(#[from] schema_registry_converter::error::SRCError),
    #[error(transparent)]
    JoinError(#[from] tokio::task::JoinError),
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
