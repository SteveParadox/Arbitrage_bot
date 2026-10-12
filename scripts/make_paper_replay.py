"""Synthetic executable-depth fixture for paper demonstration; these prices are not live quotes."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from decimal import Decimal
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("--cycles", type=int, default=300)
    parser.add_argument("--feed-interval-ms", type=int, default=1000)
    args = parser.parse_args()
    if not 20 <= args.feed_interval_ms <= 1000:
        parser.error("feed interval must be 20..1000 ms")
    if not 1 <= args.cycles <= 500000:
        parser.error("cycles must be 1..500000")
    root = Path(__file__).resolve().parents[1]
    subprocess.run(
        [
            sys.executable,
            str(root / "scripts/make_observer_replay.py"),
            str(args.directory),
        ],
        check=True,
    )
    events = [
        json.loads(line)
        for line in (args.directory / "market.jsonl").read_text().splitlines()
    ]
    events = events[:-2]
    for e in events:
        e["replay_clock"] = True
        if e["event"]["type"] == "instrument":
            e["event"]["quote_precision"] = 0.00001
            e["event"]["min_order_amt"] = 0.0001
    templates = [e for e in events if e["event"]["type"] == "order_book"]
    health = events[-1]
    metadata = [e for e in events if e["event"]["type"] == "instrument"]
    frames = list(events)
    with (args.directory / "market.jsonl").open("w") as stream:
        for initial in frames:
            stream.write(json.dumps(initial) + "\n")
        for cycle in range(args.cycles):
            for index, template in enumerate(templates):
                e = json.loads(json.dumps(template))
                e["event"]["update_id"] = e["event"]["sequence"] = 100 + cycle * 100
                e["event"]["is_snapshot"] = True
                if index == 0:
                    e["delay_ms"] = args.feed_interval_ms
                if index == 2:
                    increment = (
                        Decimal(cycle) / 1000
                        if args.feed_interval_ms == 1000
                        else Decimal(cycle) / 100000
                    )
                    e["event"]["bids"] = [
                        {"price": float(Decimal(6) + increment), "quantity": 100}
                    ]
                    e["event"]["asks"] = [
                        {"price": float(Decimal("6.1") + increment), "quantity": 100}
                    ]
                stream.write(json.dumps(e) + "\n")
            trigger = json.loads(json.dumps(e))
            if cycle and cycle % 60 == 0:
                # Explicit refreshed evidence keeps synthetic metadata valid beyond five logical minutes.
                for item in metadata:
                    stream.write(json.dumps(item) + "\n")
            stream.write(json.dumps(health) + "\n")
            trigger["event"]["update_id"] += 1
            trigger["event"]["sequence"] += 1
            stream.write(json.dumps(trigger) + "\n")
    config = json.loads((root / "shared/config/scanner.json").read_text())
    config["start_amounts"] = {"USDT": 100.0}
    (args.directory / "scanner.json").write_text(json.dumps(config))
    print(
        "Synthetic paper fixture; every fill is simulated and no exchange keys are needed"
    )


if __name__ == "__main__":
    main()
