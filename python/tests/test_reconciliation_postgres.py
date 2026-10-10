"""Real PostgreSQL multi-session contention and commit/retry tests; no exchange calls."""

import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, delete, select
from sqlalchemy.orm import Session

from analytics.db import Base
from analytics.micro_live_models import MicroLiveCycle, MicroLiveRun
from analytics.micro_live_ingest import _candidate_values
from api.micro_live import ReconcileRequest, reconcile_cycle
from api.settings import settings

URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not URL, reason="requires isolated PostgreSQL TEST_DATABASE_URL")


@pytest.fixture
def candidates():
    engine = create_engine(URL)
    Base.metadata.create_all(engine)
    run_id = "reconciliation-" + uuid.uuid4().hex
    ids = ["trade-" + uuid.uuid4().hex for _ in range(2)]
    with Session(engine) as db:
        db.add(
            MicroLiveRun(
                id=run_id,
                started_at=datetime.now(UTC),
                base_asset="USDT",
                cycle_notional=10,
                hard_cycle_cap=25,
                manual_execution_required=True,
            )
        )
        db.flush()
        for trade_id in ids:
            db.add(
                MicroLiveCycle(
                    **_candidate_values(
                        {
                            "type": "micro_live_candidate",
                            "session_id": run_id,
                            "trade_id": trade_id,
                            "detected_at_ms": int(datetime.now(UTC).timestamp() * 1000),
                            "route_id": "r",
                            "triangle_id": "t",
                            "base_asset": "USDT",
                            "starting_capital": "10",
                            "expected_pnl": "0.05",
                            "expected_fees": "0.03",
                            "expected_slippage": "0.005",
                            "expected_slippage_bps": "5",
                            "expected_net_edge_bps": "50",
                            "fee_bps_per_leg": ["10"] * 3,
                            "detection_leg_prices": [100, 0.05, 5],
                            "account_balance": "100",
                            "account_equity_usd": "100",
                            "account_exposure_usd": "0",
                            "manual_execution_required": True,
                        }
                    )
                )
            )
        db.commit()
    yield engine, run_id, ids
    with Session(engine) as db:
        db.execute(delete(MicroLiveRun).where(MicroLiveRun.id == run_id))
        db.commit()
    engine.dispose()


def payload(pnl="0.02", request_id=None):
    return ReconcileRequest(
        realized_pnl=Decimal(pnl),
        execution_time_ms=100,
        execution_status="filled",
        request_id=request_id,
    )


def execute(engine, trade_id, body):
    with Session(engine) as db:
        return reconcile_cycle(trade_id, body, db)


def test_simultaneous_same_trade_reconciliation_counts_once(candidates):
    engine, run_id, ids = candidates
    barrier = threading.Barrier(2)
    body = payload(request_id="same-" + uuid.uuid4().hex)

    def worker():
        barrier.wait(timeout=5)
        return execute(engine, ids[0], body)

    with ThreadPoolExecutor(max_workers=2) as pool:
        replies = [
            future.result(timeout=10) for future in [pool.submit(worker), pool.submit(worker)]
        ]
    assert sorted(r["duplicate"] for r in replies) == [False, True]
    with Session(engine) as db:
        assert db.get(MicroLiveRun, run_id).reconciled_cycles == 1
        assert db.get(MicroLiveCycle, ids[0]).realized_pnl == Decimal("0.02")


def test_different_trades_same_run_do_not_lose_counter_updates(candidates):
    engine, run_id, ids = candidates
    with ThreadPoolExecutor(max_workers=2) as pool:
        replies = list(pool.map(lambda trade: execute(engine, trade, payload()), ids))
    assert all(not r["duplicate"] for r in replies)
    with Session(engine) as db:
        assert db.get(MicroLiveRun, run_id).reconciled_cycles == 2


def test_retry_after_commit_is_idempotent_but_conflicting_payload_rejected(candidates):
    engine, run_id, ids = candidates
    first = execute(engine, ids[0], payload())
    # Simulate a lost HTTP response after the database commit.
    second = execute(engine, ids[0], payload("0.0200"))
    assert first["request_id"] == second["request_id"]
    assert second["duplicate"] is True
    with pytest.raises(HTTPException) as error:
        execute(engine, ids[0], payload("0.99"))
    assert error.value.status_code == 409


def test_lock_timeout_rolls_back_and_retry_succeeds(candidates, monkeypatch):
    engine, run_id, ids = candidates
    monkeypatch.setattr(settings, "arb_reconciliation_lock_timeout_ms", 50)
    with Session(engine) as blocker:
        blocker.scalar(
            select(MicroLiveCycle).where(MicroLiveCycle.trade_id == ids[0]).with_for_update()
        )
        with pytest.raises(HTTPException) as error:
            execute(engine, ids[0], payload())
        assert error.value.status_code == 503
        blocker.rollback()
    assert execute(engine, ids[0], payload())["duplicate"] is False
    with Session(engine) as db:
        assert db.get(MicroLiveRun, run_id).reconciled_cycles == 1


def test_transaction_failure_does_not_leave_financial_side_effects(candidates, monkeypatch):
    engine, run_id, ids = candidates
    from sqlalchemy.exc import SQLAlchemyError

    with Session(engine) as db:

        def lost_connection():
            raise SQLAlchemyError("injected disconnect before commit")

        monkeypatch.setattr(db, "commit", lost_connection)
        with pytest.raises(HTTPException) as error:
            reconcile_cycle(ids[0], payload(), db)
        assert error.value.status_code == 503
    with Session(engine) as db:
        assert db.get(MicroLiveCycle, ids[0]).reconciled_at is None
        assert db.get(MicroLiveRun, run_id).reconciled_cycles == 0
    assert execute(engine, ids[0], payload())["duplicate"] is False


def test_replayed_request_id_cannot_reconcile_another_trade(candidates):
    engine, _, ids = candidates
    request_id = "unique-" + uuid.uuid4().hex
    execute(engine, ids[0], payload(request_id=request_id))
    with pytest.raises(HTTPException) as error:
        execute(engine, ids[1], payload(request_id=request_id))
    assert error.value.status_code == 409
