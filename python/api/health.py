"""Bounded, conservative operational and trading-eligibility health checks."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

import redis.asyncio as redis
from sqlalchemy import desc, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from analytics.engine_event_models import EngineEvent
from api.engine_client import EngineCommandError
from api.event_consumer import consumer_metrics_snapshot
from api.operations import _risk_status, _risk_config
from api.runtime_control import read_control_state, read_stop_intent, confirm_stop
from api.settings import settings


def _latest(db: Session, event_type: str):
    return db.scalar(
        select(EngineEvent)
        .where(EngineEvent.event_type == event_type)
        .order_by(desc(EngineEvent.occurred_at_ms))
        .limit(1)
    )


def _event_pipeline_status(db: Session) -> dict[str, Any]:
    event = _latest(db, "engine.health")
    result: dict[str, Any] = {
        "status": "offline",
        "last_event_id": None,
        "last_event_at": None,
        "age_ms": None,
        "source": None,
    }
    if event is None:
        return result
    age = int(datetime.now(UTC).timestamp() * 1000) - event.occurred_at_ms
    result.update(
        last_event_id=event.event_id,
        last_event_at=datetime.fromtimestamp(event.occurred_at_ms / 1000, UTC),
        age_ms=age,
        source=event.source,
    )
    result["status"] = (
        "online"
        if 0 <= age <= settings.arb_health_event_max_age_ms
        and isinstance(event.payload, dict)
        and event.payload.get("healthy") is True
        else "stale"
    )
    return result


def _market_data_status(db: Session) -> dict[str, Any]:
    event = _latest(db, "market.health")
    risk_config = _risk_config() or {}
    risk_max_age = risk_config.get("max_market_data_age_ms")
    required = {s.strip() for s in settings.arb_market_required_symbols.split(",") if s.strip()}
    result: dict[str, Any] = {
        "status": "unknown",
        "source": "scanner market.health telemetry",
        "required_symbols": sorted(required),
        "symbols": {},
        "age_ms": None,
        "detail": "no market-data health telemetry received",
        "last_event_at": None,
    }
    if event is None:
        return result
    if type(risk_max_age) is not int or risk_max_age <= 0:
        result.update(status="degraded", detail="risk market-data freshness limit unavailable")
        return result
    max_age_ms = min(settings.arb_market_data_max_age_ms, risk_max_age)
    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    age = now_ms - event.occurred_at_ms
    payload = event.payload if isinstance(event.payload, dict) else {}
    symbols = payload.get("symbols", {})
    if not isinstance(symbols, dict):
        symbols = {}
    result.update(
        symbols=symbols,
        age_ms=age,
        last_event_at=datetime.fromtimestamp(event.occurred_at_ms / 1000, UTC),
    )
    if age < 0 or age > settings.arb_health_event_max_age_ms:
        result.update(
            status="connected_but_stale", detail="market telemetry expired or clock is ahead"
        )
    elif payload.get("state") in {"disconnected", "reconnecting", "metadata_retry"}:
        result.update(status="disconnected", detail="market connection is unavailable")
    elif (
        payload.get("state") != "connected_and_fresh"
        or payload.get("subscriptions_confirmed") is not True
        or not required
        or not required.issubset(symbols)
    ):
        result.update(
            status="resynchronizing", detail="subscriptions or required snapshots are incomplete"
        )
    else:
        usable = all(
            isinstance(symbols[s], dict)
            and symbols[s].get("initialized") is True
            and symbols[s].get("synchronized") is True
            and isinstance(symbols[s].get("exchange_timestamp_ms"), int)
            and not isinstance(symbols[s].get("exchange_timestamp_ms"), bool)
            and 0 <= now_ms - symbols[s]["exchange_timestamp_ms"] <= max_age_ms
            and type(symbols[s].get("receive_age_ms")) is int
            and 0 <= symbols[s]["receive_age_ms"] + age <= max_age_ms
            for s in required
        )
        result.update(
            status="connected_and_fresh" if usable else "connected_but_stale",
            detail="all required books synchronized and fresh"
            if usable
            else "a required book is stale or unsynchronized",
        )
    return result


async def _redis_status() -> dict[str, Any]:
    client = redis.from_url(
        settings.arb_redis_url,
        decode_responses=True,
        socket_connect_timeout=0.5,
        socket_timeout=0.5,
    )
    try:
        async with asyncio.timeout(1.0):
            await client.ping()
            groups = await client.xinfo_groups(settings.arb_event_stream)
            group = next(
                (g for g in groups if g["name"] == settings.arb_event_consumer_group), None
            )
            if group is None:
                return {
                    "status": "degraded",
                    "detail": "event consumer group is absent",
                    "pending": None,
                    "lag": None,
                }
            pending, lag = group.get("pending"), group.get("lag")
            ready = (
                isinstance(pending, int)
                and pending <= settings.arb_event_batch_size
                and isinstance(lag, int)
                and lag <= settings.arb_event_batch_size
            )
            return {
                "status": "online" if ready else "degraded",
                "pending": pending,
                "lag": lag,
                "detail": "Redis and event consumer backlog within bounds"
                if ready
                else "event processing backlog exceeds bounds or is unknown",
            }
    except (redis.RedisError, OSError, TimeoutError):
        return {
            "status": "offline",
            "pending": None,
            "lag": None,
            "detail": "Redis/event group check unavailable",
        }
    finally:
        await client.aclose()


def _detail(engine) -> dict[str, Any]:
    try:
        payload = json.loads(engine.detail)
        return payload if isinstance(payload, dict) else {}
    except (ValueError, TypeError):
        return {}


async def health_snapshot(db: Session, engine_client) -> dict[str, Any]:
    database = "online"
    market = {"status": "unknown", "detail": "database unavailable", "last_event_at": None}
    pipeline: dict[str, Any] = {
        "status": "unknown",
        "last_event_id": None,
        "last_event_at": None,
        "age_ms": None,
        "source": None,
    }
    try:
        db.execute(text("SELECT 1"))
        market = _market_data_status(db)
        pipeline = _event_pipeline_status(db)
    except SQLAlchemyError:
        try:
            db.rollback()
        except SQLAlchemyError:
            pass
        database = "offline"
    risk = _risk_status()
    control = read_control_state()
    stop = read_stop_intent()
    risk_allows = bool(
        risk.get("available") is True
        and risk.get("state") == "ready"
        and not risk.get("kill_switch_active")
        and not risk.get("circuit_breaker")
    )
    grpc_status: dict[str, Any] = {
        "status": "offline",
        "runtime_enabled": False,
        "detail": "engine status unavailable",
        "event_pipeline_status": "unknown",
        "critical_event_backlog": None,
        "event_pipeline": {},
        "grpc_idempotency_store_status": "unknown",
    }
    try:
        engine = await engine_client.status()
        timestamp = getattr(engine, "generated_at_ms", None)
        age = (
            int(datetime.now(UTC).timestamp() * 1000) - timestamp if type(timestamp) is int else -1
        )
        detail = _detail(engine)
        event = detail.get("event_pipeline", {})
        if not isinstance(event, dict):
            event = {}
        grpc_status.update(
            status="online"
            if engine.healthy and 0 <= age <= settings.arb_health_event_max_age_ms
            else "degraded",
            runtime_enabled=engine.runtime_enabled,
            strategy_generation=engine.strategy_generation,
            detail=detail.get("summary", "engine responded"),
            event_pipeline_status=event.get("event_pipeline_status", "unknown"),
            critical_event_backlog=event.get("critical_events_pending"),
            oldest_pending_event_age_ms=event.get("oldest_pending_event_age_ms"),
            event_pipeline=event,
            grpc_idempotency_store_status=detail.get("grpc_idempotency_store_status", "unknown"),
            grpc_idempotency_in_progress=detail.get("grpc_idempotency_in_progress"),
        )
    except EngineCommandError:
        pass
    # Recovery only confirms the matching engine-authored disabled operation.
    # A successful socket reconnect or an old command acknowledgement is insufficient.
    if (
        stop
        and stop["status"] != "CONFIRMED_STOPPED"
        and stop.get("request_id")
        and grpc_status["status"] == "online"
        and grpc_status["runtime_enabled"] is False
        and detail.get("control_request_id") == stop["request_id"]
    ):
        try:
            if confirm_stop(stop["request_id"]):
                stop = read_stop_intent()
                control = read_control_state()
        except (OSError, TimeoutError):
            pass
    redis_status = await _redis_status()
    dependencies = {
        "database": database == "online",
        "engine": grpc_status["status"] == "online",
        "market_data": market["status"] == "connected_and_fresh",
        "risk": risk_allows,
        "redis": redis_status["status"] == "online",
        "event_consumer": pipeline["status"] == "online",
        "event_outbox": grpc_status["event_pipeline_status"] == "healthy",
        "command_store": grpc_status["grpc_idempotency_store_status"] == "healthy",
        "control_auth": len(settings.arb_control_api_token.encode("utf-8")) >= 32,
        "control_state": control.get("valid") is True,
        "stop_resolved": not stop or stop["status"] == "CONFIRMED_STOPPED",
    }
    reasons = [name for name, ready in dependencies.items() if not ready]
    eligible = bool(
        not reasons
        and settings.arb_live_trading_enabled
        and control["enabled"]
        and grpc_status["runtime_enabled"]
    )
    operational = dependencies["database"] and dependencies["engine"] and dependencies["redis"]
    return {
        "status": "ok" if not reasons else "degraded" if operational else "unhealthy",
        "generated_at": datetime.now(UTC),
        "environment": settings.arb_env,
        "database_status": database,
        "market_stream_status": market["status"],
        "last_market_event": market["last_event_at"],
        "market_data": market,
        "event_pipeline": pipeline,
        "redis": redis_status,
        "risk": risk,
        "engine_grpc": grpc_status,
        "event_consumer": consumer_metrics_snapshot(),
        "trading": {
            "deployment_enabled": settings.arb_live_trading_enabled,
            "runtime_enabled": bool(control["enabled"] and grpc_status["runtime_enabled"]),
            "risk_allows_new_orders": risk_allows,
            "effective_enabled": eligible,
            "dependencies": dependencies,
            "blocking_reasons": reasons,
            "stop_outcome": stop["status"] if stop else None,
            "stop_request_id": stop.get("request_id") if stop else None,
            "updated_at": control["updated_at"],
            "reason": control["reason"],
            "source": control["source"],
        },
        "control_auth_configured": len(settings.arb_control_api_token.encode("utf-8")) >= 32,
    }
