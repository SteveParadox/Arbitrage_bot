from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, delete
from sqlalchemy.orm import Session

from analytics.engine_event_models import EngineEvent
from analytics.micro_live_models import MicroLiveCycle, MicroLiveRun
from analytics.models import OpportunityWindow
from analytics.performance_analytics import build_performance_analytics


DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not DATABASE_URL,
    reason="TEST_DATABASE_URL not configured",
)


def _add_engine_event(
    session: Session,
    *,
    event_id: str,
    occurred_at: datetime,
    event_type: str,
    payload: dict,
) -> None:
    session.add(
        EngineEvent(
            event_id=event_id,
            stream_id=f"{event_id}-stream",
            event_type=event_type,
            source="coordinator",
            schema_version=1,
            occurred_at_ms=int(occurred_at.timestamp() * 1000),
            payload=payload,
        )
    )


def _add_canary_cycle(
    session: Session,
    *,
    run_id: str,
    trade_id: str,
    occurred_at: datetime,
    realized_pnl: str,
    slippage_bps: str,
) -> None:
    session.add(
        MicroLiveRun(
            id=run_id,
            started_at=occurred_at - timedelta(minutes=1),
            base_asset="USDT",
            cycle_notional=Decimal("100"),
            hard_cycle_cap=Decimal("100"),
            manual_execution_required=True,
            candidates_recorded=1,
            reconciled_cycles=1,
        )
    )
    session.add(
        MicroLiveCycle(
            trade_id=trade_id,
            session_id=run_id,
            detected_at=occurred_at - timedelta(seconds=10),
            route_id=f"route-{trade_id}",
            triangle_id=f"triangle-{trade_id}",
            base_asset="USDT",
            starting_capital=Decimal("100"),
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
            execution_time_ms=40,
            execution_status="reconciled",
            notes=None,
            reconciled_at=occurred_at,
            raw_candidate_event={"trade_id": trade_id},
        )
    )


def test_phase17_aggregates_correctly_on_postgres() -> None:
    assert DATABASE_URL is not None
    engine = create_engine(DATABASE_URL)

    suffix = uuid.uuid4().hex[:12]
    window_id = f"phase17-{suffix}"
    engine_trade_id = f"engine-{suffix}"
    canary_trade_id = f"canary-{suffix}"
    attempt_event_id = f"attempt-{suffix}"
    terminal_event_id = f"terminal-{suffix}"
    duplicate_run_id = f"dup-run-{suffix}"
    canary_run_id = f"canary-run-{suffix}"
    now = datetime.now(UTC)

    try:
        with Session(engine) as session:
            session.add(
                OpportunityWindow(
                    id=window_id,
                    route_id=f"route-{suffix}",
                    triangle_id=f"triangle-{suffix}",
                    start_asset="USDT",
                    started_at=now - timedelta(minutes=10),
                    last_seen_at=now - timedelta(minutes=9),
                    ended_at=now - timedelta(minutes=9),
                    duration_ms=60_000,
                    observation_count=4,
                    max_net_edge_bps=Decimal("15"),
                    max_net_profit=Decimal("2"),
                    close_reason="expired",
                )
            )

            _add_engine_event(
                session,
                event_id=attempt_event_id,
                occurred_at=now - timedelta(minutes=8),
                event_type="trade.attempted",
                payload={
                    "trade_id": engine_trade_id,
                    "route_id": f"route-{suffix}",
                    "triangle_id": f"triangle-{suffix}",
                    "base_asset": "USDT",
                    "starting_amount": "100",
                    "opportunity_window_id": window_id,
                },
            )
            _add_engine_event(
                session,
                event_id=terminal_event_id,
                occurred_at=now - timedelta(minutes=7),
                event_type="trade.executed",
                payload={
                    "trade_id": engine_trade_id,
                    "route_id": f"route-{suffix}",
                    "base_asset": "USDT",
                    "starting_amount": "100",
                    "economic_pnl": "1.25",
                    "leg_count": 3,
                    "unwind_count": 0,
                    "estimated_turnover_base": "250",
                    "opportunity_window_id": window_id,
                    "execution_time_ms": 55,
                },
            )

            # Duplicate trade ID must lose to the terminal engine event.
            _add_canary_cycle(
                session,
                run_id=duplicate_run_id,
                trade_id=engine_trade_id,
                occurred_at=now - timedelta(minutes=6),
                realized_pnl="99",
                slippage_bps="0.9",
            )
            _add_canary_cycle(
                session,
                run_id=canary_run_id,
                trade_id=canary_trade_id,
                occurred_at=now - timedelta(minutes=5),
                realized_pnl="0.5",
                slippage_bps="0.4",
            )
            session.commit()

            analytics = build_performance_analytics(
                session,
                days=7,
                base_asset="USDT",
                bins=5,
            )

        assert analytics["funnel"]["observed_opportunities"] == 1
        assert analytics["funnel"]["expected_profitable_opportunities"] == 1
        assert analytics["funnel"]["expected_profit_total"] == pytest.approx(2.0)
        assert analytics["funnel"]["trade_attempted"] == 2
        assert analytics["funnel"]["trade_attempted_engine"] == 1
        assert analytics["funnel"]["trade_attempted_micro_canary"] == 1
        assert analytics["funnel"]["actual_profit_known"] == 2
        assert analytics["funnel"]["actual_profit_total"] == pytest.approx(1.75)
        assert analytics["funnel"]["matched_opportunity_windows"] == 1
        assert analytics["funnel"]["matched_actual_cycles"] == 1
        assert analytics["funnel"]["matched_expected_profit_total"] == pytest.approx(
            2.0
        )
        assert analytics["funnel"]["matched_actual_profit_total"] == pytest.approx(1.25)
        assert analytics["funnel"]["matched_profit_capture_pct"] == pytest.approx(62.5)

        assert analytics["profit"]["total_profit"] == pytest.approx(1.75)
        assert analytics["profit"]["profit_per_cycle"] == pytest.approx(0.875)
        assert analytics["profit"]["estimated_turnover"] == pytest.approx(550.0)
        assert analytics["profit"]["profit_per_1000_turnover"] == pytest.approx(
            1.75 / 550 * 1000
        )

        assert analytics["distributions"]["net_edge_bps"]["count"] == 1
        assert analytics["distributions"]["opportunity_survival_ms"]["count"] == 1
        assert analytics["distributions"]["slippage_bps"]["count"] == 2

        quality = analytics["data_quality"]
        assert quality["engine_trade_attempt_events"] == 1
        assert quality["engine_terminal_events"] == 1
        assert quality["engine_turnover_fallbacks"] == 0
        assert quality["engine_attributed_terminal_events"] == 1
        assert quality["micro_canary_cycles_loaded"] == 2
        assert quality["micro_canary_cycles"] == 1
        assert quality["deduplicated_canary_trade_ids"] == 1
        assert quality["profit_metrics_sampled"] is False
    finally:
        with Session(engine) as session:
            session.execute(
                delete(MicroLiveCycle).where(
                    MicroLiveCycle.trade_id.in_(
                        [engine_trade_id, canary_trade_id]
                    )
                )
            )
            session.execute(
                delete(MicroLiveRun).where(
                    MicroLiveRun.id.in_([duplicate_run_id, canary_run_id])
                )
            )
            session.execute(
                delete(EngineEvent).where(
                    EngineEvent.event_id.in_(
                        [attempt_event_id, terminal_event_id]
                    )
                )
            )
            session.execute(
                delete(OpportunityWindow).where(
                    OpportunityWindow.id == window_id
                )
            )
            session.commit()
        engine.dispose()
