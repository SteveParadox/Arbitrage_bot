#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    Paper,
    Live,
}

pub fn live_execution_allowed(_enabled: bool) -> bool {
    // No live execution implementation has been validated in Phases 1–8.
    false
}

#[cfg(test)]
mod tests {
    #[test]
    fn live_execution_is_never_enabled() {
        assert!(!super::live_execution_allowed(false));
        assert!(!super::live_execution_allowed(true));
    }
}
