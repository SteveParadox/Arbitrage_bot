import os
from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from analytics.db import Base
from analytics.opportunity_store import OpportunityStore, analytics_summary
from simulator import models as simulator_models  # noqa: F401


DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not DATABASE_URL, reason="TEST_DATABASE_URL not configured")


def scan(timestamp_ms: int, net_bps: float, accepted: bool) -> dict:
    return {
        "scan_timestamp": timestamp_ms,
        "trigger_symbol": "ETHUSDT",
        "trigger_update_id": timestamp_ms,
        "trigger_sequence": timestamp_ms,
        "route_id": "USDT>BTC>ETH>USDT",
        "triangle_id": "BTC-ETH-USDT",
        "start_asset": "USDT",
        "start_amount": 450.0,
        "final_amount": 450.5 if accepted else 449.5,
        "gross_profit": 1.0 if accepted else -0.5,
        "gross_return_bps": 22.0 if accepted else -11.0,
        "gross_return_pct": 0.22 if accepted else -0.11,
        "gross_profitable": accepted,
        "expected_net_profit": 0.5 if accepted else -1.0,
        "expected_net_return_bps": net_bps,
        "expected_net_return_pct": net_bps / 100,
        "expected_final_amount": 450.5 if accepted else 449.0,
        "net_profitable": accepted,
        "profitability": {
            "fee_amount": 1.35,
            "fee_bps_on_start": 30.0,
            "expected_slippage_amount": 0.2,
            "expected_slippage_bps": 5.0,
            "rounding_loss_amount": 0.0,
            "rounding_loss_bps": 0.0,
            "latency_buffer_amount": 0.1,
            "latency_buffer_bps": 3.0,
            "safety_margin_amount": 0.2,
            "safety_margin_bps": 5.0,
            "total_cost_amount": 1.85,
            "total_cost_bps": 43.0,
        },
        "status": "complete",
        "reason": None,
        "book_timestamp_skew_ms": 2,
        "fees_included": True,
        "execution_enabled": False,
        "legs": [
            {
                "side": "BUY",
                "complete": True,
                "execution": {
                    "requested_quote_quantity": 450.0,
                    "filled_quote_quantity": 450.0,
                },
            },
            {
                "side": "BUY",
                "complete": True,
                "execution": {
                    "requested_quote_quantity": 1.0,
                    "filled_quote_quantity": 1.0,
                },
            },
            {
                "side": "SELL",
                "complete": True,
                "execution": {
                    "requested_base_quantity": 1.0,
                    "filled_base_quantity": 1.0,
                },
            },
        ],
    }


def test_postgres_funnel_and_deduplication() -> None:
    engine = create_engine(DATABASE_URL)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    now_ms = int(datetime.now(UTC).timestamp() * 1000)

    with Session(engine) as session:
        store = OpportunityStore(session, max_continuity_gap_ms=2_000)
        assert store.record_scan(scan(now_ms, 12.0, True)) is True
        assert store.record_scan(scan(now_ms, 12.0, True)) is False
        assert store.record_scan(scan(now_ms + 500, -4.0, False)) is True
        session.commit()

        summary = analytics_summary(session, 1)
        assert summary["detected"] == 2
        assert summary["executable"] == 2
        assert summary["accepted"] == 1
        assert summary["rejected"] == 1
        assert summary["opportunity_windows"] == 1
        assert summary["max_window_duration_ms"] == 500

    Base.metadata.drop_all(engine)
