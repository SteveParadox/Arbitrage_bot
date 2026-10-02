from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from analytics.db import Base, get_db
from analytics.engine_event_models import EngineEvent
from analytics.micro_live_models import MicroLiveCycle, MicroLiveRun
from analytics.models import OpportunityWindow
from analytics.performance_analytics import (
    ActualCycle,
    _decimal,
    _distribution,
    _engine_cycles,
    _quantile,
    build_performance_analytics,
)
from api.performance_analytics import router as performance_router


@pytest.fixture
def db_factory():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(
        engine,
        tables=[
            OpportunityWindow.__table__,
            EngineEvent.__table__,
            MicroLiveRun.__table__,
            MicroLiveCycle.__table__,
        ],
    )
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        engine.dispose()


def _ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _add_engine_event(
    db,
    *,
    event_id: str,
    occurred_at: datetime,
    event_type: str,
    payload: dict,
) -> None:
    db.add(
        EngineEvent(
            event_id=event_id,
            stream_id=f"{event_id}-stream",
            event_type=event_type,
            source="coordinator",
            schema_version=1,
            occurred_at_ms=_ms(occurred_at),
            payload=payload,
        )
    )


def _add_canary_cycle(
    db,
    *,
    trade_id: str,
    occurred_at: datetime,
    realized_pnl: str,
    starting_capital: str = "100",
    slippage_bps: str = "0.5",
    execution_time_ms: int = 35,
) -> None:
    session_id = f"run-{trade_id}"
    db.add(
        MicroLiveRun(
            id=session_id,
            started_at=occurred_at - timedelta(minutes=1),
            base_asset="USDT",
            cycle_notional=Decimal(starting_capital),
            hard_cycle_cap=Decimal(starting_capital),
            manual_execution_required=True,
            candidates_recorded=1,
            reconciled_cycles=1,
        )
    )
    db.add(
        MicroLiveCycle(
            trade_id=trade_id,
            session_id=session_id,
            detected_at=occurred_at - timedelta(seconds=10),
            route_id=f"route-{trade_id}",
            triangle_id=f"triangle-{trade_id}",
            base_asset="USDT",
            starting_capital=Decimal(starting_capital),
            expected_pnl=Decimal("1"),
            expected_fees=Decimal("0.1"),
            expected_slippage=Decimal("0.05"),
            expected_slippage_bps=Decimal("0.5"),
            expected_net_edge_bps=Decimal("10"),
            fee_bps_per_leg=[1, 1, 1],
            detection_leg_prices=[1, 1, 1],
            account_balance=Decimal("1000"),
            account_equity_usd=Decimal("1000"),
            account_exposure_usd=Decimal("0"),
            manual_execution_required=True,
            realized_pnl=Decimal(realized_pnl),
            prediction_error=Decimal("0"),
            actual_fee_amount_base=Decimal("0.1"),
            actual_fees_by_currency={"USDT": "0.1"},
            actual_slippage=Decimal("0.05"),
            actual_slippage_bps=Decimal(slippage_bps),
            execution_time_ms=execution_time_ms,
            execution_status="reconciled",
            notes=None,
            reconciled_at=occurred_at,
            raw_candidate_event={"trade_id": trade_id},
        )
    )


def test_distribution_reports_quantiles_and_histogram() -> None:
    distribution = _distribution(
        [1.0, 2.0, 3.0, 4.0],
        bins=2,
        total_count=4,
        unit="bps",
    )

    assert distribution["count"] == 4
    assert distribution["sample_count"] == 4
    assert distribution["median"] == 2.5
    assert distribution["p25"] == 1.75
    assert sum(item["count"] for item in distribution["bins"]) == 4


def test_distribution_handles_constant_values() -> None:
    distribution = _distribution(
        [5.0, 5.0, 5.0],
        bins=10,
        total_count=3,
        unit="ms",
    )

    assert distribution["mean"] == 5.0
    assert distribution["bins"] == [
        {"lower": 5.0, "upper": 5.0, "count": 3}
    ]


def test_quantile_single_value_is_stable() -> None:
    assert _quantile([9.0], 0.95) == 9.0


def test_decimal_parser_rejects_invalid_values() -> None:
    assert _decimal("1.25") == Decimal("1.25")
    assert _decimal("not-a-number") is None
    assert _decimal(None) is None


def test_actual_cycle_is_immutable_value_object() -> None:
    cycle = ActualCycle(
        trade_id="trade-1",
        opportunity_window_id=None,
        occurred_at=datetime.now(UTC),
        base_asset="USDT",
        starting_amount=Decimal("10"),
        pnl=Decimal("0.10"),
        execution_time_ms=50,
        estimated_turnover=Decimal("30"),
        source="engine",
        successful=True,
    )
    assert cycle.pnl == Decimal("0.10")


def test_distribution_count_can_exceed_sample_count() -> None:
    distribution = _distribution(
        [1.0, 2.0],
        bins=2,
        total_count=10,
        unit="bps",
    )

    assert distribution["count"] == 10
    assert distribution["sample_count"] == 2


def test_build_performance_analytics_tracks_funnel_and_deduplicates(db_factory) -> None:
    now = datetime.now(UTC)
    with db_factory() as db:
        db.add_all(
            [
                OpportunityWindow(
                    id="window-profitable",
                    route_id="route-a",
                    triangle_id="triangle-a",
                    start_asset="USDT",
                    started_at=now - timedelta(minutes=10),
                    last_seen_at=now - timedelta(minutes=9),
                    ended_at=now - timedelta(minutes=9),
                    duration_ms=60_000,
                    observation_count=4,
                    max_net_edge_bps=Decimal("12"),
                    max_net_profit=Decimal("1.5"),
                    close_reason="expired",
                ),
                OpportunityWindow(
                    id="window-open",
                    route_id="route-b",
                    triangle_id="triangle-b",
                    start_asset="USDT",
                    started_at=now - timedelta(minutes=8),
                    last_seen_at=now - timedelta(minutes=1),
                    ended_at=None,
                    duration_ms=420_000,
                    observation_count=7,
                    max_net_edge_bps=Decimal("-2"),
                    max_net_profit=Decimal("-0.2"),
                    close_reason=None,
                ),
            ]
        )

        _add_engine_event(
            db,
            event_id="attempt-1",
            occurred_at=now - timedelta(minutes=7),
            event_type="trade.attempted",
            payload={
                "trade_id": "trade-1",
                "base_asset": "USDT",
                "starting_amount": "100",
            },
        )
        _add_engine_event(
            db,
            event_id="terminal-1",
            occurred_at=now - timedelta(minutes=6),
            event_type="trade.executed",
            payload={
                "trade_id": "trade-1",
                "base_asset": "USDT",
                "starting_amount": "100",
                "economic_pnl": "1",
                "leg_count": 3,
                "unwind_count": 0,
                "estimated_turnover_base": "250",
                "opportunity_window_id": "window-profitable",
                "execution_time_ms": 50,
            },
        )
        _add_engine_event(
            db,
            event_id="terminal-2",
            occurred_at=now - timedelta(minutes=5),
            event_type="trade.failed",
            payload={
                "trade_id": "trade-2",
                "base_asset": "USDT",
                "starting_amount": "100",
                "leg_count": 1,
                "unwind_count": 1,
                "execution_time_ms": 70,
            },
        )
        _add_engine_event(
            db,
            event_id="orphan-order",
            occurred_at=now - timedelta(minutes=4),
            event_type="order.executed",
            payload={"trade_id": "orphan-trade"},
        )

        _add_canary_cycle(
            db,
            trade_id="trade-1",
            occurred_at=now - timedelta(minutes=3),
            realized_pnl="99",
            slippage_bps="0.9",
        )
        _add_canary_cycle(
            db,
            trade_id="trade-3",
            occurred_at=now - timedelta(minutes=2),
            realized_pnl="2",
            slippage_bps="0.4",
        )
        db.commit()

        analytics = build_performance_analytics(
            db,
            days=7,
            base_asset="USDT",
            bins=5,
        )

    assert analytics["funnel"]["observed_opportunities"] == 2
    assert analytics["funnel"]["expected_profitable_opportunities"] == 1
    assert analytics["funnel"]["expected_profit_total"] == pytest.approx(1.5)
    assert analytics["funnel"]["trade_attempted"] == 3
    assert analytics["funnel"]["trade_attempted_engine"] == 2
    assert analytics["funnel"]["trade_attempted_micro_canary"] == 1
    assert analytics["funnel"]["actual_profit_known"] == 2
    assert analytics["funnel"]["actual_profit_total"] == pytest.approx(3.0)
    assert analytics["funnel"]["attempt_to_actual_known_pct"] == pytest.approx(
        200 / 3
    )
    assert analytics["funnel"]["matched_opportunity_windows"] == 1
    assert analytics["funnel"]["matched_actual_cycles"] == 1
    assert analytics["funnel"]["matched_expected_profit_total"] == pytest.approx(1.5)
    assert analytics["funnel"]["matched_actual_profit_total"] == pytest.approx(1.0)
    assert analytics["funnel"]["matched_profit_capture_pct"] == pytest.approx(
        200 / 3
    )

    assert analytics["profit"]["total_profit"] == pytest.approx(3.0)
    assert analytics["profit"]["profit_per_cycle"] == pytest.approx(1.5)
    assert analytics["profit"]["estimated_turnover"] == pytest.approx(550.0)
    assert analytics["profit"]["profit_per_1000_turnover"] == pytest.approx(
        3000 / 550
    )
    assert analytics["profit"]["by_source"]["engine"]["cycles"] == 1
    assert analytics["profit"]["by_source"]["micro_canary"]["cycles"] == 1

    assert analytics["distributions"]["win_loss"]["wins"] == 2
    assert analytics["distributions"]["win_loss"]["unknown_pnl"] == 1
    assert analytics["distributions"]["opportunity_survival_ms"]["count"] == 1
    assert analytics["distributions"]["opportunity_survival_ms"]["open_windows"] == 1
    assert analytics["distributions"]["slippage_bps"]["count"] == 2

    quality = analytics["data_quality"]
    assert quality["engine_trade_attempt_events"] == 1
    assert quality["engine_terminal_events"] == 2
    assert quality["engine_turnover_fallbacks"] == 1
    assert quality["engine_attributed_terminal_events"] == 1
    assert quality["micro_canary_cycles_loaded"] == 2
    assert quality["micro_canary_cycles"] == 1
    assert quality["deduplicated_canary_trade_ids"] == 1
    assert quality["orphan_order_attempts_without_terminal_trade"] == 1
    assert quality["profit_metrics_sampled"] is False


def test_historical_turnover_fallback_includes_unwind_orders(db_factory) -> None:
    now = datetime.now(UTC)
    with db_factory() as db:
        _add_engine_event(
            db,
            event_id="legacy-terminal",
            occurred_at=now - timedelta(minutes=1),
            event_type="trade.executed",
            payload={
                "trade_id": "legacy-trade",
                "base_asset": "USDT",
                "starting_amount": "100",
                "realized_base_pnl": "1",
                "leg_count": 2,
                "unwind_count": 2,
            },
        )
        db.commit()

        cycles, attempted_ids, _, _, _, fallback_count = _engine_cycles(
            db,
            start_ms=_ms(now - timedelta(hours=1)),
            base_asset="USDT",
            limit=100,
        )

    assert attempted_ids == {"legacy-trade"}
    assert len(cycles) == 1
    assert cycles[0].estimated_turnover == Decimal("400")
    assert fallback_count == 1


def test_performance_endpoint_exposes_phase17_contract(db_factory) -> None:
    app = FastAPI()
    app.include_router(performance_router)

    def override_db():
        with db_factory() as db:
            yield db

    app.dependency_overrides[get_db] = override_db
    client = TestClient(app)

    response = client.get(
        "/analytics/performance",
        params={"days": 7, "base_asset": "USDT", "bins": 10},
    )

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {
        "generated_at",
        "window",
        "profit",
        "funnel",
        "distributions",
        "data_quality",
    }
    assert "engine_turnover_fallbacks" in body["data_quality"]
    assert "micro_canary_cycles_loaded" in body["data_quality"]

    invalid = client.get("/analytics/performance", params={"days": 0})
    assert invalid.status_code == 422
