import os
import json
import subprocess
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from analytics.db import Base
from analytics.models import OpportunityObservation, OpportunityWindow
from analytics.opportunity_store import OpportunityStore
from simulator.book_archive import load_symbol_replays
from simulator.models import MarketBookEvent
from simulator.book_ingest import _book_values
from simulator.replay import simulate_route
from strategy.triangle_discovery import Instrument, build_config, discover_triangles
from test_opportunity_postgres import scan

DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not DATABASE_URL, reason="TEST_DATABASE_URL not configured")


@pytest.fixture
def session():
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    with Session(engine) as value:
        yield value
        value.rollback()
    engine.dispose()


def test_late_and_duplicate_observations_do_not_rewind_windows(session):
    now = int(datetime.now(UTC).timestamp()*1000)
    store = OpportunityStore(session)
    first, later = scan(now, 12, True), scan(now+500, 12, True)
    assert store.record_scan(first)
    assert store.record_scan(later)
    late = scan(now-1000, 12, True)
    assert store.record_scan(late)
    assert not store.record_scan(later)
    windows = session.scalars(select(OpportunityWindow)).all()
    assert len(windows) == 1
    assert windows[0].observation_count == 2
    assert windows[0].duration_ms == 500
    assert int(windows[0].last_seen_at.timestamp()*1000) == now+500
    assert session.scalar(select(OpportunityObservation).where(
        OpportunityObservation.scan_timestamp_ms == now-1000
    )).opportunity_window_id is None


def test_silent_gap_and_rejection_last_seen(session):
    now = int(datetime.now(UTC).timestamp()*1000)
    store = OpportunityStore(session, max_continuity_gap_ms=100)
    for offset, accepted in [(0, True), (500, True), (550, False)]:
        assert store.record_scan(scan(now+offset, 12 if accepted else -1, accepted))
    windows = session.scalars(select(OpportunityWindow).order_by(OpportunityWindow.started_at)).all()
    assert [w.duration_ms for w in windows] == [0, 50]
    assert windows[0].close_reason == "continuity_gap"
    assert int(windows[1].last_seen_at.timestamp()*1000) == now+500


def test_independent_store_instances_reload_committed_window_state(session):
    now = int(datetime.now(UTC).timestamp()*1000)
    first, second = OpportunityStore(session), OpportunityStore(session)
    first.record_scan(scan(now, 12, True))
    second.record_scan(scan(now+50, 12, True))
    first.record_scan(scan(now+100, 12, True))
    windows = session.scalars(select(OpportunityWindow)).all()
    assert len(windows) == 1 and windows[0].observation_count == 3


def test_archive_history_budget_and_invalid_history_are_explicit(session):
    session.add_all([MarketBookEvent(symbol="BTCUSDT", event_timestamp_ms=1000+i,
        update_id=10+i, sequence=10+i, is_snapshot=i == 0,
        bids=[[99, 1]], asks=[[100, 1]]) for i in range(3)])
    session.flush()
    replays = load_symbol_replays(session, {"BTCUSDT"}, start_ms=1000, end_ms=1010,
                                  max_history_events=2)
    assert replays["BTCUSDT"].failure_reason == "history_limit_exceeded"
    assert replays["BTCUSDT"].state_at(1010) is None
    session.add(MarketBookEvent(symbol="BAD", event_timestamp_ms=1000,
        update_id=10, sequence=10, is_snapshot=True, bids=[[0, 1]], asks=[]))
    session.flush()
    replay = load_symbol_replays(session, {"BAD"}, start_ms=1000, end_ms=1010)["BAD"]
    assert replay.failure_reason == "invalid_book_history"


def test_python_routes_rust_scanner_postgres_archive_and_replay(session, tmp_path):
    root = Path(__file__).resolve().parents[2]
    binary = root / "rust/target/debug/scan-live"
    if not binary.exists():
        pytest.skip("build scanner --bin scan-live to exercise the cross-language pipeline")
    result = discover_triangles([
        Instrument("BTCUSDT", "BTC", "USDT"), Instrument("ETHBTC", "ETH", "BTC"),
        Instrument("ETHUSDT", "ETH", "USDT")], {"USDT"})
    triangles = tmp_path / "triangles.json"
    triangles.write_text(json.dumps(build_config(result, testnet=True, start_assets={"USDT"})))
    settings = tmp_path / "scanner.json"
    settings.write_text(json.dumps({"version": 1, "start_amounts": {"USDT": 450},
        "record_path": str(tmp_path / "scans.ndjson"),
        "profitability_config_path": str(root / "shared/config/profitability.json")}))
    now = int(datetime.now(UTC).timestamp()*1000)-10
    events = [{"type": "order_book", "symbol": symbol, "timestamp": now,
               "update_id": 10, "sequence": 10, "is_snapshot": True,
               "bids": [{"price": bid, "quantity": 100}],
               "asks": [{"price": ask, "quantity": 100}]}
              for symbol, bid, ask in [("BTCUSDT", 99., 100.),
                  ("ETHBTC", .049, .05), ("ETHUSDT", 5.25, 5.3)]]
    process = subprocess.run([str(binary)], input="\n".join(map(json.dumps, events)),
        capture_output=True, text=True, check=True,
        env={**os.environ, "ARB_TRIANGLE_CONFIG": str(triangles),
             "ARB_SCANNER_CONFIG": str(settings)})
    scans = [json.loads(line) for line in process.stdout.splitlines()]
    assert len(scans) == 6  # two directed routes, three book updates
    store = OpportunityStore(session)
    for value in scans:
        assert store.record_scan(value)
        assert not store.record_scan(value)
    complete = [value for value in scans if value["status"] == "complete"]
    assert len(complete) == 2
    accepted = session.scalars(select(OpportunityObservation).where(
        OpportunityObservation.accepted.is_(True))).all()
    assert len(accepted) == 1
    assert accepted[0].gross_final_amount == Decimal("472.5")
    for value in events:
        session.add(MarketBookEvent(**_book_values(value)))
    session.flush()
    histories = load_symbol_replays(session, {event["symbol"] for event in events},
        start_ms=now, end_ms=now+1600)
    for latency in (25, 50, 100, 200, 500):
        replay_result = simulate_route(accepted[0].raw_scan, histories, latency_ms=latency,
            fee_bps_per_leg=(Decimal("10"),)*3, max_book_age_ms=2000)
        assert replay_result.completed
        assert replay_result.final_amount == Decimal("472.5") * Decimal(".999")**3
