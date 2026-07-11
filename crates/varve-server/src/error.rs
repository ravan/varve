#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error(transparent)]
    Engine(#[from] varve_engine::EngineError),
    #[error(transparent)]
    Rows(#[from] varve::RowError),
    #[error(transparent)]
    Base64(#[from] base64::DecodeError),
    #[error(transparent)]
    Config(#[from] varve_config::ConfigError),
    #[error(transparent)]
    Registry(#[from] varve_config::RegistryError),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("unauthorized")]
    Unauthorized,
    #[error("forbidden")]
    Forbidden,
    #[error("not acceptable: {0}")]
    NotAcceptable(String),
    #[error("writer advertisement is missing")]
    MissingWriterAdvertisement,
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Http(#[from] axum::http::Error),
}
