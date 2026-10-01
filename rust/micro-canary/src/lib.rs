mod config;
mod model;

pub use config::{
    load_micro_canary_config, MicroCanaryConfig, ABSOLUTE_MAX_CYCLE_NOTIONAL,
};
pub use model::{MicroCanaryCandidate, MicroCanaryRun};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum MicroCanaryError {
    #[error("invalid micro-canary configuration: {0}")]
    InvalidConfig(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}
