#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    Paper,
    Live,
}

pub fn live_execution_allowed(enabled: bool) -> bool {
    enabled
}
