"""Synthetic executable-depth fixture for paper demonstration; these prices are not live quotes."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("--cycles", type=int, default=300)
    args = parser.parse_args()
    if not 1 <= args.cycles <= 3600:
        parser.error("cycles must be 1..3600")
    root = Path(__file__).resolve().parents[1]
    subprocess.run([sys.executable, str(root/"scripts/make_observer_replay.py"), str(args.directory)],
                   check=True)
    events = [json.loads(line) for line in (args.directory/"market.jsonl").read_text().splitlines()]
    events = events[:-2]
    for e in events:
        e["replay_clock"] = True
        if e["event"]["type"] == "instrument":
            e["event"]["quote_precision"] = .00001
            e["event"]["min_order_amt"] = .0001
    templates = [e for e in events if e["event"]["type"] == "order_book"]
    health = events[-1]
    for cycle in range(args.cycles):
        for index, template in enumerate(templates):
            e = json.loads(json.dumps(template))
            e["event"]["update_id"] = e["event"]["sequence"] = 100+cycle*100
            e["event"]["is_snapshot"] = True
            if index == 0:
                e["delay_ms"] = 1000
            if index == 2:
                e["event"]["bids"] = [{"price": 6+cycle/1000, "quantity": 100}]
                e["event"]["asks"] = [{"price": 6.1+cycle/1000, "quantity": 100}]
            events.append(e)
        events.append(json.loads(json.dumps(health)))
        trigger = json.loads(json.dumps(events[-2]))
        trigger["event"]["update_id"] += 1
        trigger["event"]["sequence"] += 1
        events.append(trigger)
    (args.directory/"market.jsonl").write_text("".join(json.dumps(e)+"\n" for e in events))
    config = json.loads((root/"shared/config/scanner.json").read_text())
    config["start_amounts"] = {"USDT": 100.0}
    (args.directory/"scanner.json").write_text(json.dumps(config))
    print("Synthetic paper fixture; every fill is simulated and no exchange keys are needed")


if __name__ == "__main__":
    main()
