pub mod config;
pub mod connector;
pub mod model;
pub mod orderbook;

pub fn service_name() -> &'static str {
    "market-data"
}
