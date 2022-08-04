#[macro_export]
macro_rules! n {
    ($iri:expr) => {
        oxigraph::model::NamedNodeRef::new_unchecked($iri)
    };
}

type N = oxigraph::model::NamedNodeRef<'static>;

pub mod dcterms {
    use super::N;

    pub const FORMAT: N = n!("http://purl.org/dc/terms/format");
}

pub mod dcat {
    use super::N;

    pub const DATASET_CLASS: N = n!("http://www.w3.org/ns/dcat#Dataset");
    pub const DISTRIBUTION_CLASS: N = n!("http://www.w3.org/ns/dcat#Distribution");
    pub const DISTRIBUTION: N = n!("http://www.w3.org/ns/dcat#distribution");
    pub const ACCESS_URL: N = n!("http://www.w3.org/ns/dcat#accessURL");
    pub const DOWNLOAD_URL: N = n!("http://www.w3.org/ns/dcat#downloadURL");
}

pub mod dqv {
    use super::N;

    pub const QUALITY_MEASUREMENT_CLASS: N = n!("http://www.w3.org/ns/dqv#QualityMeasurement");
    pub const IS_MEASUREMENT_OF: N = n!("http://www.w3.org/ns/dqv#isMeasurementOf");
    pub const COMPUTED_ON: N = n!("http://www.w3.org/ns/dqv#computedOn");
    pub const VALUE: N = n!("http://www.w3.org/ns/dqv#value");
}

pub mod dcat_mqa {
    use super::N;

    pub const ASSESSMENT_OF: N = n!("https://data.norge.no/vocabulary/dcatno-mqa#assessmentOf");
    pub const HAS_ASSESSMENT: N = n!("https://data.norge.no/vocabulary/dcatno-mqa#hasAssessment");
    pub const CONTAINS_QUALITY_MEASUREMENT: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement");
    pub const DATASET_ASSESSMENT_CLASS: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#DatasetAssessment");
    pub const DISTRIBUTION_ASSESSMENT_CLASS: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#DistributionAssessment");
    pub const HAS_DISTRIBUTION_ASSESSMENT: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#hasDistributionAssessment");
    pub const ACCESS_URL_STATUS_CODE: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#accessUrlStatusCode");
    pub const DOWNLOAD_URL_STATUS_CODE: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#downloadUrlStatusCode");
}
