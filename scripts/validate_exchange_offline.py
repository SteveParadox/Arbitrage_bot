"""Run isolated real-service integrations without exchange credentials or orders.

PostgreSQL and Redis must already be running on literal loopback addresses.
Creates disposable databases; never flushes the supplied Redis instance.
"""
from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import psycopg
import redis


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--redis-url", required=True)
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()
    source = args.database_url.replace("postgresql+psycopg:", "postgresql:")
    db_url, redis_url = urlsplit(source), urlsplit(args.redis_url)
    if db_url.hostname not in {"127.0.0.1", "::1"} or redis_url.hostname not in {"127.0.0.1", "::1"}:
        parser.error("offline validation requires literal local PostgreSQL and Redis")
    root = Path(__file__).resolve().parents[1]
    binary = root / "rust/target/debug/engine-service"
    if not binary.is_file():
        parser.error("build the Rust workspace first")
    redis.Redis.from_url(args.redis_url, socket_timeout=2).ping()
    names = ["arb_offline_" + uuid.uuid4().hex for _ in range(2)]
    admin_url = urlunsplit(db_url._replace(path="/postgres"))
    env = {key: value for key, value in os.environ.items() if not key.startswith("BYBIT_") and not key.startswith("ARB_")}
    engine = None
    with tempfile.TemporaryDirectory(prefix="arb-offline-") as directory, psycopg.connect(admin_url, autocommit=True) as admin:
        directory = Path(directory)
        created = []
        try:
            for name in names:
                admin.execute('CREATE DATABASE "' + name + '"')
                created.append(name)
            database = urlunsplit(db_url._replace(path="/" + names[0])).replace("postgresql:", "postgresql+psycopg:")
            env.update(ARB_ENV="development", ARB_TRADING_MODE="observe", ARB_LIVE_TRADING_ENABLED="false",
                       ARB_TESTNET_TRADING_ENABLED="false", ARB_PAPER_TRADING_ENABLED="false",
                       ARB_DATABASE_URL=database, ARB_REDIS_URL=args.redis_url, ARB_RUN_EXCHANGE_INTEGRATION="1",
                       ARB_CONTROL_API_TOKEN="offline-control-" + "c" * 32,
                       PYTHONPATH=str(root / "python") + ":" + str(root / "python/tests"))

            def run(command, cwd=root):
                subprocess.run(command, cwd=cwd, env=env, check=True)

            run([sys.executable, "-m", "alembic", "upgrade", "head"], root / "python")
            if args.full:
                env.update(TEST_DATABASE_URL=urlunsplit(db_url._replace(path="/" + names[1])).replace("postgresql:", "postgresql+psycopg:"),
                           ARB_RUN_PAPER_INTEGRATION="1", ARB_RUN_OBSERVER_INTEGRATION="1",
                           ARB_RUN_GRPC_INTEGRATION="1", ARB_RUN_RELIABILITY_INTEGRATION="1",
                           ARB_ENGINE_BINARY=str(binary), ARB_ENGINE_AUTOSTART="false",
                           ARB_ENGINE_GRPC_ADDR="127.0.0.1:55052", ARB_ENGINE_GRPC_TARGET="127.0.0.1:55052",
                           ARB_ENGINE_GRPC_TOKEN="offline-internal-" + "g" * 32,
                           ARB_TEST_GRPC_RESPONSE_DELAY_MS="200",
                           ARB_CONTROL_STATE_FILE=str(directory / "control.json"),
                           ARB_RUNTIME_LIMITS_FILE=str(directory / "limits.json"),
                           ARB_STRATEGY_RELOAD_FILE=str(directory / "reload.json"),
                           ARB_GRPC_IDEMPOTENCY_STORE=str(directory / "commands"),
                           ARB_EVENT_OUTBOX_PATH=str(directory / "outbox"),
                           ARB_EVENT_STREAM="arb.offline." + uuid.uuid4().hex,
                           ARB_ENGINE_PID_FILE=str(directory / "engine.pid"),
                           ARB_ENGINE_LOG_FILE=str(directory / "engine.log"))
                with (directory / "engine.log").open("ab") as log:
                    engine = subprocess.Popen([str(binary)], cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)
                (directory / "engine.pid").write_text(str(engine.pid))
                time.sleep(.5)
                if engine.poll() is not None:
                    raise RuntimeError((directory / "engine.log").read_text())
                run(["cargo", "test", "--manifest-path", "rust/Cargo.toml", "-p", "event-bus", "--", "--ignored"])
            run([sys.executable, str(root / "scripts/verify_paper_migrations.py")])
            tests = [] if args.full else ["tests/test_exchange_runtime.py", "tests/test_execution_security.py"]
            run([sys.executable, "-m", "pytest", "-q", *tests], root / "python")
            print("Offline validation passed; no authenticated exchange orders were submitted.")
        finally:
            if engine:
                pid_file = directory / "engine.pid"
                try:
                    os.kill(int(pid_file.read_text()), signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    engine.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    engine.kill()
                    engine.wait()
            for name in created:
                admin.execute('DROP DATABASE "' + name + '" WITH (FORCE)')


if __name__ == "__main__":
    main()
