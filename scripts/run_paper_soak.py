"""Bounded synthetic replay through engine-service, real PostgreSQL/Redis and FastAPI.

Requires migrated isolated services and a built development Rust engine. Never calls
private Bybit APIs. Prints measured results, including failed and unresolved cycles.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import httpx
import psycopg

ROOT = Path(__file__).resolve().parents[1]


def wait(check, seconds=20):
    end = time.monotonic()+seconds
    while time.monotonic() < end:
        try:
            if result := check():
                return result
        except (httpx.HTTPError, psycopg.Error):
            pass
        time.sleep(.05)
    raise RuntimeError("runtime or API failed readiness")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--latencies", default="25,50,100,200,500")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not 3 <= args.seconds <= 3600:
        parser.error("seconds must be 3..3600 per latency")
    required = {"ARB_TRADING_MODE": "paper", "ARB_PAPER_TRADING_ENABLED": "true",
                "ARB_LIVE_TRADING_ENABLED": "false", "ARB_ENV": "development"}
    if any(os.environ.get(k) != v for k, v in required.items()):
        parser.error("requires explicit development paper authorization and live=false")
    database = os.environ["ARB_DATABASE_URL"].replace("postgresql+psycopg:", "postgresql:")
    os.environ["ARB_REDIS_URL"]  # required, no implicit local service
    token = os.environ["ARB_CONTROL_API_TOKEN"]
    internal_token = os.environ["ARB_ENGINE_GRPC_TOKEN"]
    results = []
    with tempfile.TemporaryDirectory(prefix="arb-paper-soak-") as temp:
        temp = Path(temp)
        api_env = dict(os.environ, PYTHONPATH=str(ROOT/"python"), ARB_EVENT_STREAM="arb.paper.soak.api", ARB_ENGINE_GRPC_TARGET="127.0.0.1:55054", ARB_ENGINE_GRPC_TOKEN=internal_token)
        with (temp/"api.log").open("w") as log:
            api = subprocess.Popen([sys.executable, "-m", "uvicorn", "api.main:app", "--host", "127.0.0.1", "--port", "58082"], cwd=ROOT/"python", env=api_env, stdout=log, stderr=subprocess.STDOUT)
            try:
                with httpx.Client(base_url="http://127.0.0.1:58082", trust_env=False, headers={"Authorization": "Bearer "+token}) as client:
                    wait(lambda: client.get("/health").status_code == 200)
                    for latency in map(int, args.latencies.split(",")):
                        account = "soak-"+uuid.uuid4().hex[:12]
                        directory = temp/account
                        subprocess.run([sys.executable, str(ROOT/"scripts/make_paper_replay.py"), str(directory), "--cycles", str(min(3600, args.seconds+2))], env=api_env, check=True, stdout=subprocess.DEVNULL)
                        response = client.post("/paper/accounts", json={"account_id":account,"amount":"500","request_id":str(uuid.uuid4())})
                        response.raise_for_status()
                        config = json.loads((ROOT/"shared/config/risk.json").read_text())
                        config.update(kill_switch_file=str(directory/"GLOBAL_KILL"),
                                      trading_control_file=str(directory/"global-control.json"),
                                      runtime_limits_file=str(directory/"global-limits.json"))
                        (directory/"risk.json").write_text(json.dumps(config))
                        env = dict(api_env, ARB_RISK_CONFIG=str(directory/"risk.json"), ARB_ENGINE_AUTOSTART="true", ARB_PAPER_ACCOUNT=account, ARB_ENGINE_GRPC_ADDR="127.0.0.1:55054", ARB_ENGINE_GRPC_TOKEN=internal_token,
                                   ARB_TRIANGLE_CONFIG=str(directory/"triangles.json"), ARB_SCANNER_CONFIG=str(directory/"scanner.json"), ARB_OBSERVER_REPLAY_FILE=str(directory/"market.jsonl"),
                                   ARB_PAPER_LATENCY_MS=str(latency), ARB_EVENT_STREAM="arb.paper.soak.api", ARB_EVENT_OUTBOX_PATH=str(directory/"outbox"), ARB_OBSERVER_LOCK=str(directory/"observer.lock"),
                                   ARB_GRPC_IDEMPOTENCY_STORE=str(directory/"commands"), ARB_CONTROL_STATE_FILE=str(directory/"control.json"), ARB_RUNTIME_LIMITS_FILE=str(directory/"limits.json"))
                        binary = os.environ.get("ARB_ENGINE_BINARY", str(ROOT/"rust/target/debug/engine-service"))
                        with (directory/"engine.log").open("w") as engine_log:
                            engine = subprocess.Popen([binary], cwd=ROOT, env=env, stdout=engine_log, stderr=subprocess.STDOUT)
                            try:
                                wait(lambda account=account: client.get(f"/paper/{account}/status").json().get("runtime_online"))
                                response = client.post(f"/paper/{account}/control/start", json={"request_id":str(uuid.uuid4()),"reason":"explicit bounded synthetic replay"})
                                response.raise_for_status()
                                started = time.monotonic()
                                while time.monotonic()-started < args.seconds:
                                    if engine.poll() is not None:
                                        raise RuntimeError((directory/"engine.log").read_text()[-4000:])
                                    time.sleep(.25)
                                elapsed = time.monotonic()-started
                                status = client.get(f"/paper/{account}/status").json()
                                rejection_events = client.get(f"/paper/{account}/events?limit=500").json()
                                performance = client.get(f"/paper/{account}/performance").json()
                                cycles = client.get(f"/paper/{account}/cycles?limit=500").json()
                                orders = client.get(f"/paper/{account}/orders?limit=500").json()
                                if latency <= 100 and args.seconds >= 5 and sum(c["state"]=="COMPLETED" for c in cycles)<2:
                                    raise AssertionError({"missing":"multiple autonomous cycles", "cycles":cycles,"rejections":rejection_events[:3]})
                                with psycopg.connect(database) as connection:
                                    t = time.monotonic()
                                    connection.execute("SELECT 1").fetchone()
                                    query_ms = (time.monotonic()-t)*1000
                                    counts = dict(connection.execute("SELECT kind,count(*) FROM paper_events WHERE account_id=%s GROUP BY kind", (account,)).fetchall())
                                transport_health = client.get("/health").json()
                                results.append({"account":account,"latency_ms":latency,"actual_seconds":elapsed,"max_rss_kib":None,"cpu_seconds":None,
                                                "postgres_probe_ms":query_ms,"status":status,"performance":performance,"cycle_states":{s:sum(c["state"]==s for c in cycles) for s in {c["state"] for c in cycles}},
                                                "transport_health":transport_health,"partial_orders":sum(o["result"] is not None and o["result"]["status"]=="PartiallyFilledCanceled" for o in orders),"journal_counts":counts,"rejections":[e for e in rejection_events if e["kind"]=="candidate.rejected"],
                                                "first_cycle":cycles[-1] if cycles else None,"first_orders":[o for o in orders if cycles and o["cycle_id"]==cycles[-1]["id"]]})
                                client.post(f"/paper/{account}/control/stop", json={"request_id":str(uuid.uuid4()),"reason":"bounded run complete"}).raise_for_status()
                            finally:
                                if engine.poll() is None:
                                    engine.send_signal(signal.SIGINT)
                                    try:
                                        _, status_code, usage = os.wait4(engine.pid, 0)
                                        engine.returncode = os.waitstatus_to_exitcode(status_code)
                                        if results and results[-1]["account"] == account:
                                            results[-1]["cpu_seconds"] = usage.ru_utime+usage.ru_stime
                                            results[-1]["max_rss_kib"] = usage.ru_maxrss
                                            results[-1]["resource_source"] = "wait4 child process resource accounting"
                                    except (OSError, AttributeError):
                                        engine.wait(timeout=20)
                                if results and results[-1]["account"] == account:
                                    results[-1]["after_graceful_shutdown"] = {
                                        "status": client.get(f"/paper/{account}/status").json(),
                                        "performance": client.get(f"/paper/{account}/performance").json(),
                                        "reconciliation": client.get(f"/paper/{account}/reconciliation").json()}

            finally:
                api.terminate()
                api.wait(timeout=10)
    report = {"source":"synthetic executable-depth fixtures; not live Bybit prices", "simulated":True,"runs":results}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, default=str)+"\n")
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
