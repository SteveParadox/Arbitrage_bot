"""Run the existing integrated validator/soak with disposable local services.

Development-only infrastructure. Production TLS is tested by the separate TLS suite.
Run as an ordinary OS user. ARB_TEST_PG_PRELOAD is a test-only escape for restricted
sandboxes; it is never passed to application clients and proves no container security.
"""

from __future__ import annotations
import argparse
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time

import psycopg
from redis import Redis

ROOT = Path(__file__).resolve().parents[1]


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("full", "soak"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--profile",
        choices=("smoke", "extended", "medium", "long", "restart", "fault"),
        default="smoke",
    )
    parser.add_argument("--seconds", type=int)
    parser.add_argument(
        "--pg-bin", type=Path, default=Path("/usr/lib/postgresql/16/bin")
    )
    parser.add_argument(
        "--pg-share", type=Path, default=Path("/usr/share/postgresql/16")
    )
    parser.add_argument("--redis-bin", default="redis-server")
    args = parser.parse_args()
    if os.getuid() == 0 and not os.getenv("ARB_TEST_PG_PRELOAD"):
        parser.error("PostgreSQL requires a non-root OS user")
    args.output.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    processes = []
    logs = []
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("ARB_", "BYBIT_", "LD_PRELOAD"))
    }
    env.update(ARB_ENV="development", ARB_LIVE_TRADING_ENABLED="false")
    pg_env = dict(env)
    if preload := os.getenv("ARB_TEST_PG_PRELOAD"):
        pg_env["LD_PRELOAD"] = preload
    pg_port, redis_port = free_port(), free_port()
    database = f"postgresql://audit@127.0.0.1:{pg_port}/postgres"
    redis = f"redis://127.0.0.1:{redis_port}/0"
    result = {
        "checked_at": datetime.now(UTC).isoformat(),
        "source_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "action": args.action,
        "profile": args.profile,
        "live_trading_enabled": False,
        "infrastructure": "disposable PostgreSQL and Redis on literal loopback; development plaintext only",
        "root_sandbox_shim": bool(os.getenv("ARB_TEST_PG_PRELOAD")),
    }
    temporary_context = tempfile.TemporaryDirectory(prefix="arb-validation-")
    temporary = temporary_context.name
    try:
        temporary = Path(temporary)
        data = temporary / "postgres"
        subprocess.run(
            [
                str(args.pg_bin / "initdb"),
                "-D",
                str(data),
                "-L",
                str(args.pg_share),
                "-U",
                "audit",
                "-A",
                "trust",
                "--no-locale",
                "--encoding=UTF8",
            ],
            env=pg_env,
            check=True,
            capture_output=True,
        )
        for name, command, environment in [
            (
                "postgres",
                [
                    str(args.pg_bin / "postgres"),
                    "-D",
                    str(data),
                    "-k",
                    "",
                    "-p",
                    str(pg_port),
                    "-c",
                    "listen_addresses=127.0.0.1",
                    "-c",
                    "max_connections=250",
                    "-c",
                    "jit=off",
                ],
                pg_env,
            ),
            (
                "redis",
                [
                    args.redis_bin,
                    "--bind",
                    "127.0.0.1",
                    "--port",
                    str(redis_port),
                    "--save",
                    "",
                    "--appendonly",
                    "no",
                    "--dir",
                    str(temporary),
                ],
                env,
            ),
        ]:
            log = (args.output / (name + ".log")).open("w")
            logs.append(log)
            processes.append(
                subprocess.Popen(
                    command, env=environment, stdout=log, stderr=subprocess.STDOUT
                )
            )
        deadline = time.monotonic() + 20
        while True:
            try:
                with psycopg.connect(
                    database, autocommit=True, connect_timeout=2
                ) as connection:
                    connection.execute("SELECT 1")
                Redis.from_url(redis, socket_timeout=2).ping()
                break
            except Exception:
                if time.monotonic() > deadline:
                    raise RuntimeError("disposable services failed startup") from None
                time.sleep(0.1)
        if args.action == "full":
            command = [
                sys.executable,
                str(ROOT / "scripts/validate_exchange_offline.py"),
                "--database-url",
                database,
                "--redis-url",
                redis,
                "--full",
            ]
        else:
            env.update(
                ARB_TRADING_MODE="paper",
                ARB_PAPER_TRADING_ENABLED="true",
                ARB_TESTNET_TRADING_ENABLED="false",
                ARB_DATABASE_URL=database.replace("/postgres", "/soak").replace(
                    "postgresql:", "postgresql+psycopg:"
                ),
                ARB_REDIS_URL=redis,
                PYTHONPATH=str(ROOT / "python"),
            )
            import secrets

            env.update(
                ARB_CONTROL_API_TOKEN=secrets.token_hex(32),
                ARB_ENGINE_GRPC_TOKEN=secrets.token_hex(32),
            )
            with psycopg.connect(database, autocommit=True) as connection:
                connection.execute("CREATE DATABASE soak")
            subprocess.run(
                [sys.executable, "-m", "alembic", "upgrade", "head"],
                cwd=ROOT / "python",
                env=env,
                check=True,
                capture_output=True,
            )
            command = [
                sys.executable,
                str(ROOT / "scripts/run_paper_soak.py"),
                "--profile",
                args.profile,
                "--latencies",
                "50",
                "--output",
                str(args.output / "soak.json"),
            ]
            if args.seconds:
                command.extend(["--seconds", str(args.seconds)])
        with (args.output / "validation.log").open("w") as log:
            completed = subprocess.run(
                command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT
            )
        result.update(
            status="PASS" if completed.returncode == 0 else "FAIL",
            exit_code=completed.returncode,
        )
    except Exception as error:
        result.update(status="BLOCKED", failure_category=type(error).__name__)
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        for log in logs:
            log.close()
        temporary_context.cleanup()
        result["actual_seconds"] = time.monotonic() - started
        (args.output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))
    raise SystemExit(0 if result["status"] == "PASS" else 1)


if __name__ == "__main__":
    main()
