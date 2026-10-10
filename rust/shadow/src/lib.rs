mod account;
mod config;
mod engine;
mod model;

pub use account::{ReadOnlyAccountClient, ReadOnlyAccountSnapshot, ReadOnlyFeeRate};

pub use config::{load_shadow_config, ShadowConfig};
pub use engine::ShadowEngine;
pub use model::{ShadowEvent, ShadowLatencySample, ShadowOpportunity};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ShadowError {
    #[error("invalid shadow configuration: {0}")]
    InvalidConfig(String),
    #[error("read-only account error: {0}")]
    Account(String),
    #[error("shadow engine error: {0}")]
    Engine(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}
