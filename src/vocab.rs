pub mod dcterms {
    use oxigraph::model::NamedNodeRef;

    pub const FORMAT: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/format");
}

pub mod dcat {
    use oxigraph::model::NamedNodeRef;

    pub const DATASET_CLASS: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dcat#Dataset");

    pub const DISTRIBUTION_CLASS: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dcat#Distribution");

    pub const DISTRIBUTION: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dcat#distribution");

    pub const ACCESS_URL: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dcat#accessURL");

    pub const DOWNLOAD_URL: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dcat#downloadURL");
}

pub mod dqv {
    use oxigraph::model::NamedNodeRef;

    pub const QUALITY_MEASUREMENT_CLASS: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dqv#QualityMeasurement");

    pub const HAS_QUALITY_MEASUREMENT: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dqv#hasQualityMeasurement");

    pub const IS_MEASUREMENT_OF: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dqv#isMeasurementOf");

    pub const COMPUTED_ON: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dqv#computedOn");

    pub const VALUE: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dqv#value");
}

pub mod dcat_mqa {
    use oxigraph::model::NamedNodeRef;

    pub const ASSESSMENT_OF: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dcat#assessmentOf");

    pub const DATASET_ASSESSMENT_CLASS: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dcat#DatasetAssessment");

    pub const DISTRIBUTION_ASSESSMENT_CLASS: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dcat#DistributionAssessment");

    pub const HAS_DISTRIBUTION_ASSESSMENT: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dcat#hasDistributionAssessment");

    pub const ACCESS_URL_STATUS_CODE: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "https://data.norge.no/vocabulary/dcatno-mqa#accessUrlStatusCode",
    );

    pub const DOWNLOAD_URL_STATUS_CODE: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "https://data.norge.no/vocabulary/dcatno-mqa#downloadUrlStatusCode",
    );
}
