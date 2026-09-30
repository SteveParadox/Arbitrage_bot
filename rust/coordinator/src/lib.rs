mod authorizer;
mod config;
mod coordinator;
mod model;
mod planner;
mod venue;

pub use authorizer::{
    LegAuthorizationContext, RiskEngineAuthorizer, RiskIntentProvider, RouteRiskAuthorizer,
};
pub use config::{load_coordinator_config, CoordinatorConfig};
pub use coordinator::ThreeLegCoordinator;
pub use model::{
    ConversionLeg, CoordinatorStatus, Holdings, LegExecutionReport, PlannedOrder,
    RouteExecutionReport, UnwindExecutionReport,
};
pub use planner::{LiveBookPlanner, RoutePlanner};
pub use venue::CoordinatorVenue;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoordinatorError {
    #[error("invalid coordinator configuration: {0}")]
    InvalidConfig(String),
    #[error("invalid route: {0}")]
    InvalidRoute(String),
    #[error("planning failed: {0}")]
    Planning(String),
    #[error("risk authorization failed: {0}")]
    Risk(String),
    #[error("execution failed: {0}")]
    Execution(String),
    #[error("order state is unresolved: {0}")]
    Unresolved(String),
    #[error("emergency unwind failed: {0}")]
    Unwind(String),
    #[error("numeric conversion failed: {0}")]
    Numeric(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}
