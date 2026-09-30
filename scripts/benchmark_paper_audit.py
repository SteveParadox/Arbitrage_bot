"""Synthetic PostgreSQL/replay scale check, never a profitability backtest.

Requires an EMPTY disposable database whose name starts with arbitrage_benchmark.
Does not delete data. Run with PYTHONPATH=python from repository root.
"""
import argparse
import json
import resource
import time
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import create_engine, func, select
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

from analytics.db import Base
from analytics.models import OpportunityObservation
from simulator.models import MarketBookEvent, PaperSimulationResult
from simulator.paper_trade import DEFAULT_LATENCIES, run_simulation


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--opportunities", type=int, default=10000)
    args = parser.parse_args()
    if not (make_url(args.database_url).database or "").startswith("arbitrage_benchmark"):
        raise ValueError("use an explicitly named disposable arbitrage_benchmark database")
    if not 0 < args.opportunities <= 100000:
        raise ValueError("opportunities must be between 1 and 100000")
    engine = create_engine(args.database_url)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        if session.scalar(select(func.count()).select_from(OpportunityObservation)):
            raise ValueError("benchmark database is not empty; no data changed")
        base_ms = int(datetime.now(UTC).timestamp() * 1000) - args.opportunities - 5000
        for offset in range(0, args.opportunities + 1600, 100):
            for symbol, bid, ask in [("BTCUSDT", 99, 100), ("ETHBTC", .049, .05),
                                      ("ETHUSDT", 5.25, 5.3)]:
                session.add(MarketBookEvent(symbol=symbol, event_timestamp_ms=base_ms+offset,
                    update_id=offset+1, sequence=offset+1, is_snapshot=True,
                    bids=[[bid, 100]], asks=[[ask, 100]]))
        rows = []
        for i in range(args.opportunities):
            timestamp = base_ms+i
            raw = {"scan_timestamp": timestamp, "start_amount": 450, "final_amount": 472.5,
                   "legs": [{"symbol": symbol, "side": side,
                             "execution": {"average_execution_price": price}}
                            for symbol, side, price in [("BTCUSDT", "BUY", 100),
                                ("ETHBTC", "BUY", .05), ("ETHUSDT", "SELL", 5.25)]]}
            rows.append(dict(observation_key=f"synthetic-{i}",
                detected_at=datetime.fromtimestamp(timestamp/1000, UTC),
                scan_timestamp_ms=timestamp, trigger_symbol="ETHUSDT", trigger_update_id=i+1,
                trigger_sequence=i+1, route_id="USDT>BTC>ETH>USDT", triangle_id="BTC-ETH-USDT",
                start_asset="USDT", starting_capital=450, gross_final_amount=472.5,
                net_profit=20, net_edge_bps=444.44444444, opportunity_duration_ms=0,
                scanner_status="complete", executable=True, accepted=True,
                fees_included=True, raw_scan=raw))
            if len(rows) == 500:
                session.execute(OpportunityObservation.__table__.insert(), rows)
                rows.clear()
        if rows:
            session.execute(OpportunityObservation.__table__.insert(), rows)
        session.commit()
        started = time.perf_counter()
        run = run_simulation(session, hours=24, limit=args.opportunities,
            latencies=DEFAULT_LATENCIES, max_book_age_ms=1000, checkpoint_interval=100,
            chunk_size=500, profitability_config=Path(__file__).resolve().parents[1]
            / "shared/config/profitability.json", include_rejected=False)
        count = session.scalar(select(func.count()).select_from(PaperSimulationResult))
        assert count == args.opportunities*5
        print(json.dumps({"synthetic_only": True, "opportunities": args.opportunities,
            "scenarios": count, "seconds": round(time.perf_counter()-started, 3),
            "peak_process_rss_mib": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024, 2),
            "fills_by_latency": {key: value["fills"] for key, value in
                                 run.summary["latency_scenarios"].items()}}, indent=2))


if __name__ == "__main__":
    main()
