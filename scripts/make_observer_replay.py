"""Create a deterministic three-book observer replay with current timestamps.

The fixture is public-data-only. The engine rejects replay unless development
mode and the live-trading deployment gate are explicitly disabled.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from strategy.triangle_discovery import Instrument, build_config, discover_triangles


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    args.directory.mkdir(parents=True, exist_ok=True)
    instruments = [
        Instrument("BTCUSDT", "BTC", "USDT"),
        Instrument("ETHBTC", "ETH", "BTC"),
        Instrument("ETHUSDT", "ETH", "USDT"),
    ]
    config = build_config(
        discover_triangles(instruments, {"USDT"}),
        testnet=True,
        start_assets={"USDT"},
    )
    # Keep one executable conversion direction for a compact replay.
    route = next(route for route in config["routes"] if route["id"] == "USDT>BTC>ETH>USDT")
    config["routes"] = [route]
    config["route_count"] = 1
    config["triangle_count"] = 1
    (args.directory / "triangles.json").write_text(json.dumps(config), encoding="utf-8")

    now = int(time.time() * 1000)
    events: list[dict] = []
    for instrument in instruments:
        events.append(
            {
                "type": "instrument",
                "symbol": instrument.symbol,
                "status": "Trading",
                "base_coin": instrument.base_asset,
                "quote_coin": instrument.quote_asset,
                "settle_coin": None,
                "tick_size": 0.00001,
                "qty_step": 0.00001,
                "min_order_qty": 0.00001,
                "timestamp": now,
            }
        )
    events.append({"type": "status", "state": "connected", "detail": "replay", "timestamp": now})
    books = [
        ("BTCUSDT", 99.0, 100.0, 10.0),
        ("ETHBTC", 0.049, 0.05, 100.0),
        ("ETHUSDT", 6.0, 6.1, 100.0),
    ]
    for sequence, (symbol, bid, ask, quantity) in enumerate(books, 1):
        events.append(
            {
                "type": "order_book",
                "symbol": symbol,
                "bids": [{"price": bid, "quantity": quantity}],
                "asks": [{"price": ask, "quantity": quantity}],
                "timestamp": now,
                "update_id": sequence,
                "sequence": sequence,
                "is_snapshot": True,
            }
        )
    events.append(
        {
            "type": "health",
            "state": "connected_and_fresh",
            "timestamp": now,
            "subscriptions_confirmed": True,
            "symbols": {
                symbol: {
                    "initialized": True,
                    "synchronized": True,
                    "exchange_timestamp_ms": now,
                    "receive_age_ms": 0,
                }
                for symbol, *_ in books
            },
        }
    )
    final = dict(events[-2])
    final["update_id"] = 4
    final["sequence"] = 4
    final["is_snapshot"] = False
    events.append(final)
    rejected = dict(final)
    rejected["update_id"] = 5
    rejected["sequence"] = 5
    rejected["bids"] = [
        {"price": 6.0, "quantity": 0.0},
        {"price": 4.0, "quantity": 100.0},
    ]
    rejected["asks"] = [
        {"price": 6.1, "quantity": 0.0},
        {"price": 4.1, "quantity": 100.0},
    ]
    events.append(rejected)
    (args.directory / "market.jsonl").write_text(
        "".join(json.dumps(event, separators=(",", ":")) + "\n" for event in events),
        encoding="utf-8",
    )
    print(args.directory / "triangles.json")
    print(args.directory / "market.jsonl")


if __name__ == "__main__":
    main()
