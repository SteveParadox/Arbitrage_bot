"""Authenticated aggregate telemetry; identities belong in audit records, not labels."""

from __future__ import annotations

import hmac
import time

from fastapi import APIRouter, HTTPException, Request, Response
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from redis import Redis
from sqlalchemy import text

from analytics.db import get_engine
from api.infrastructure import atomic_json, read_file, redis_options
from api.settings import settings

router = APIRouter()
REQUESTS = Counter(
    "arb_api_requests_total", "Bounded API request classes", ["route", "method", "status"]
)
LATENCY = Histogram(
    "arb_api_request_seconds",
    "API duration",
    ["route"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
)
AUTH_FAILURES = Counter(
    "arb_api_auth_failures_total", "Authentication/authorization denials", ["status"]
)
DEPENDENCY = Gauge("arb_dependency_up", "Verified dependency probe", ["service"])
DEPENDENCY_LATENCY = Gauge("arb_dependency_probe_seconds", "Last probe duration", ["service"])
FINANCIAL = Gauge(
    "arb_financial_integrity_ok", "Independent persisted financial proof is healthy and fresh"
)
RECONCILIATION = Gauge(
    "arb_reconciliation_age_seconds", "Age of latest authenticated reconciliation"
)
UNRESOLVED = Gauge("arb_unresolved_orders", "Orders without confirmed terminal exchange evidence")
CYCLES = Gauge("arb_active_cycles", "Unsettled cycles", ["mode"])
RESERVATION_AGE = Gauge(
    "arb_oldest_reservation_seconds", "Oldest unsettled paper or exchange cycle"
)
CAPITAL = Gauge("arb_paper_capital", "Aggregated simulated capital only", ["asset", "scope"])
ENGINE = Gauge("arb_engine_up", "Fresh supervised engine heartbeat")
SCANNER = Gauge("arb_scanner_ready", "Fresh synchronized scanner evidence")
PIPELINE = Gauge("arb_event_pipeline_ok", "Fresh durable publisher health")
BACKLOG = Gauge("arb_redis_stream_backlog", "Consumer pending plus undispatched stream entries")
REDIS_MEMORY = Gauge("arb_redis_memory_bytes", "Redis used memory")
DB_CONNECTIONS = Gauge("arb_database_connections", "PostgreSQL total connections")
DB_LOCKS = Gauge("arb_database_waiting_locks", "PostgreSQL locks awaiting grant")
POOL = Gauge("arb_database_pool_checked_out", "Application checked-out connections")
ENGINE_RESOURCE = Gauge("arb_engine_resource", "Process self-reporting resource values", ["kind"])
CERT_EXPIRY = Gauge(
    "arb_tls_certificate_expiry_seconds", "Certificate expiry interval", ["service"]
)
BACKUP_AGE = Gauge("arb_backup_age_seconds", "Age of latest verified backup")
EXCHANGE_REST = Gauge(
    "arb_exchange_rest_requests", "Per-process authenticated request counts", ["endpoint", "result"]
)


def route_group(path):
    return next(
        (
            prefix[1:]
            for prefix in ("/paper", "/execution", "/trading", "/health", "/metrics", "/livez")
            if path == prefix or path.startswith(prefix + "/")
        ),
        "other",
    )


def proof(path, field, seconds):
    import json

    try:
        data = json.loads(read_file(path))
        stamp = data.get("checked_at")
        return (
            type(stamp) in (int, float)
            and 0 <= time.time() - stamp <= seconds
            and data.get(field) is True
        )
    except (ValueError, TypeError):
        return False


def refresh():
    import json

    now = time.time()
    database_ok = redis_ok = False
    engine_ok = scanner_ok = publisher_ok = False
    t = time.monotonic()
    try:
        engine = get_engine()
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
            database_ok = True
            DB_CONNECTIONS.set(
                connection.execute(text("SELECT count(*) FROM pg_stat_activity")).scalar_one()
            )
            DB_LOCKS.set(
                connection.execute(
                    text("SELECT count(*) FROM pg_locks WHERE NOT granted")
                ).scalar_one()
            )
            for mode, table in (
                ("paper", "paper_cycles"),
                ("testnet", "execution_cycles"),
                ("live", "execution_cycles"),
            ):
                condition = (
                    "settled=false"
                    if mode == "paper"
                    else "NOT settled AND account_id IN (SELECT id FROM execution_accounts WHERE mode=:mode)"
                )
                CYCLES.labels(mode).set(
                    connection.execute(
                        text(f"SELECT count(*) FROM {table} WHERE {condition}"), {"mode": mode}
                    ).scalar_one()
                )
            UNRESOLVED.set(
                connection.execute(
                    text(
                        "SELECT count(*) FROM execution_orders WHERE state NOT IN ('FILLED','CANCELLED','REJECTED','EXPIRED')"
                    )
                ).scalar_one()
            )
            age = connection.execute(
                text(
                    "SELECT EXTRACT(EPOCH FROM now()-max(created_at)) FROM execution_reconciliation WHERE healthy=true"
                )
            ).scalar_one()
            RECONCILIATION.set(age if age is not None else float("inf"))
            age = connection.execute(
                text(
                    "SELECT EXTRACT(EPOCH FROM now()-min(created_at)) FROM (SELECT created_at FROM paper_cycles WHERE NOT settled UNION ALL SELECT created_at FROM execution_cycles WHERE NOT settled) x"
                )
            ).scalar_one()
            RESERVATION_AGE.set(age or 0)
            for asset in ("USDT", "BTC", "ETH"):
                balances = connection.execute(
                    text(
                        "SELECT COALESCE(sum(available),0),COALESCE(sum(reserved),0) FROM paper_balances WHERE asset=:asset"
                    ),
                    {"asset": asset},
                ).one()
                for scope, value in zip(("available", "reserved"), balances, strict=True):
                    CAPITAL.labels(asset, scope).set(value)
            for event_type in ("engine.health", "engine.observer"):
                row = connection.execute(
                    text(
                        "SELECT payload,occurred_at_ms FROM engine_events WHERE event_type=:kind ORDER BY occurred_at_ms DESC LIMIT 1"
                    ),
                    {"kind": event_type},
                ).first()
                if row and 0 <= now - row.occurred_at_ms / 1000 <= 15:
                    payload = row.payload
                    if event_type == "engine.health":
                        engine_ok = payload.get("healthy") is True
                        publisher_ok = (
                            payload.get("event_pipeline", {}).get("event_pipeline_status")
                            == "healthy"
                        )
                    else:
                        scanner_ok = payload.get("scanner_ready") is True
        POOL.set(engine.pool.checkedout())
    except Exception:
        # Failure must invalidate previously published readiness, even if one early query succeeded.
        database_ok = engine_ok = scanner_ok = publisher_ok = False
    finally:
        DEPENDENCY.labels("postgresql").set(database_ok)
        DEPENDENCY_LATENCY.labels("postgresql").set(time.monotonic() - t)
    t = time.monotonic()
    try:
        with Redis.from_url(
            settings.arb_redis_url, decode_responses=True, **redis_options(settings)
        ) as client:
            redis_ok = bool(client.ping())
            REDIS_MEMORY.set(client.info("memory")["used_memory"])
            groups = client.xinfo_groups(settings.arb_event_stream)
            group = next(
                (g for g in groups if g["name"] == settings.arb_event_consumer_group), None
            )
            if group is None or group.get("lag") is None:
                redis_ok = False
                BACKLOG.set(float("inf"))
            else:
                BACKLOG.set(group["pending"] + group["lag"])
    except Exception:
        redis_ok = False
    finally:
        DEPENDENCY.labels("redis").set(redis_ok)
        DEPENDENCY_LATENCY.labels("redis").set(time.monotonic() - t)
    ENGINE.set(engine_ok)
    SCANNER.set(scanner_ok)
    PIPELINE.set(publisher_ok)
    balanced = proof(settings.arb_financial_integrity_file, "balanced", 120)
    FINANCIAL.set(balanced)
    try:
        sample = json.loads(read_file(settings.arb_engine_metrics_file))
        if 0 <= now - sample["checked_at"] <= 15:
            for name in (
                "rss_bytes",
                "threads",
                "open_fds",
                "cpu_ticks",
                "read_bytes",
                "write_bytes",
            ):
                if name in sample:
                    ENGINE_RESOURCE.labels(name).set(sample[name])
            for endpoint, counts in sample.get("exchange_rest", {}).items():
                if endpoint in (
                    "create",
                    "cancel",
                    "realtime",
                    "history",
                    "executions",
                    "wallet",
                    "account",
                    "time",
                    "other",
                ):
                    for result in ("requests", "errors", "rate_limits", "auth_errors"):
                        EXCHANGE_REST.labels(endpoint, result).set(counts.get(result, 0))
    except (ValueError, KeyError, TypeError):
        pass
    from cryptography import x509
    from datetime import UTC, datetime

    for name, variable in (
        ("postgresql", "ARB_MONITOR_POSTGRES_CERT"),
        ("redis", "ARB_MONITOR_REDIS_CERT"),
        ("grpc", "ARB_ENGINE_GRPC_TLS_CERT"),
        ("https", "ARB_MONITOR_HTTPS_CERT"),
    ):
        import os

        if path := os.getenv(variable):
            try:
                certificate = x509.load_pem_x509_certificate(read_file(path, limit=1048576))
                CERT_EXPIRY.labels(name).set(
                    (certificate.not_valid_after_utc - datetime.now(UTC)).total_seconds()
                )
            except ValueError:
                CERT_EXPIRY.labels(name).set(0)
    try:
        backup = json.loads(read_file(settings.arb_backup_status_file))
        BACKUP_AGE.set(
            now - backup["checked_at"] if backup.get("verified") is True else float("inf")
        )
    except (ValueError, KeyError, TypeError):
        BACKUP_AGE.set(float("inf"))
    ready = database_ok and redis_ok and engine_ok and scanner_ok and publisher_ok and balanced
    atomic_json(
        settings.arb_monitoring_readiness_file,
        {
            "checked_at": time.time(),
            "ready_for_new_exposure": ready,
            "database": database_ok,
            "redis": redis_ok,
            "financial_integrity": balanced,
            "engine": engine_ok,
            "scanner": scanner_ok,
            "publisher": publisher_ok,
        },
    )


@router.get("/metrics", include_in_schema=False)
def metrics(request: Request):
    from api.security import bearer

    token = settings.arb_metrics_token
    if len(token.encode()) < 32 or not hmac.compare_digest(
        token.encode(), bearer(request.headers).encode()
    ):
        raise HTTPException(401, "monitoring credential required")
    refresh()
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
