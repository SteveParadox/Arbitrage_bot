pub fn within_notional_limit(order_notional: f64, max_notional: f64) -> bool {
    order_notional >= 0.0 && order_notional <= max_notional
}
