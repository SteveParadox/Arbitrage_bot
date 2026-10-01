from decimal import Decimal

from analytics.micro_live_ingest import _candidate_values


def test_candidate_keeps_expected_calibration_fields() -> None:
    event = {
        "type": "micro_live_candidate",
        "session_id": "micro-canary-1",
        "trade_id": "trade-1",
        "detected_at_ms": 1000,
        "route_id": "USDT>BTC>ETH>USDT",
        "triangle_id": "BTC-ETH-USDT",
        "base_asset": "USDT",
        "starting_capital": "10",
        "expected_pnl": "0.05",
        "expected_fees": "0.03",
        "expected_slippage": "0.005",
        "expected_slippage_bps": "5",
        "expected_net_edge_bps": "50",
        "fee_bps_per_leg": ["10", "10", "10"],
        "detection_leg_prices": [68000.0, 0.05, 3400.0],
        "account_balance": "100",
        "account_equity_usd": "125",
        "account_exposure_usd": "25",
        "manual_execution_required": True,
    }
    values = _candidate_values(event)
    assert values["starting_capital"] == Decimal("10")
    assert values["expected_pnl"] == Decimal("0.05")
    assert values["expected_fees"] == Decimal("0.03")
    assert values["manual_execution_required"] is True
