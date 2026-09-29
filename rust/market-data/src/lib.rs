pub fn service_name() -> &'static str {
    "market-data"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposes_service_name() {
        assert_eq!(service_name(), "market-data");
    }
}
