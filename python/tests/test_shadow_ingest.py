from decimal import Decimal

from analytics.shadow_ingest import _opportunity_values, _sample_values


def test_parses_shadow_opportunity_decimal_fields() -> None:
    event = {
        "type": "opportunity",
        "run_id": "run-1",
        "observation_id": "abc",
        "detected_at_ms": 1_000,
        "route_id": "USDT>BTC>ETH>USDT",
        "triangle_id": "BTC-ETH-USDT",
        "start_asset": "USDT",
        "starting_capital": "450",
        "detection_final_amount": "452.8",
        "detection_gross_profit": "2.8",
        "expected_profit": "0.81",
        "latency_neutral_detection_profit": "0.945",
        "expected_net_edge_bps": "18",
        "detected": True,
        "approved": True,
        "would_execute": True,
        "approval_error": None,
        "risk_checks": [],
        "account_balance": "1000",
        "account_equity_usd": "1200",
        "account_exposure_usd": "200",
        "session_pnl_proxy_usd": "-1.25",
        "detection_leg_prices": [68000.0, 0.05, 3400.0],
        "oldest_book_timestamp_ms": 990,
        "newest_book_timestamp_ms": 995,
        "book_timestamp_skew_ms": 5,
        "latency_tracking": True,
    }

    values = _opportunity_values(event)

    assert values["starting_capital"] == Decimal("450")
    assert values["expected_profit"] == Decimal("0.81")
    assert values["session_pnl_proxy_usd"] == Decimal("-1.25")
    assert values["approved"] is True


def test_parses_latency_sample_without_inventing_missing_profit() -> None:
    event = {
        "type": "latency_sample",
        "run_id": "run-1",
        "observation_id": "abc",
        "route_id": "USDT>BTC>ETH>USDT",
        "latency_ms": 100,
        "target_at_ms": 1_100,
        "sampled_at_ms": 1_103,
        "scheduler_lag_ms": 3,
        "sample_valid": False,
        "failure_reason": "missing book",
        "final_amount": None,
        "net_profit": None,
        "net_edge_bps": None,
        "profit_drift_from_detection": None,
        "route_final_drift_bps": None,
        "leg_price_drift_bps": [],
        "profitable_after_latency": False,
        "still_meets_min_edge": False,
        "leg_average_prices": [],
        "oldest_book_timestamp_ms": None,
        "newest_book_timestamp_ms": None,
        "book_timestamp_skew_ms": None,
    }

    values = _sample_values(event)

    assert values["latency_ms"] == 100
    assert values["net_profit"] is None
    assert values["sample_valid"] is False
