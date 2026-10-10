from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
from dataclasses import dataclass
from decimal import Decimal

import redis.asyncio as redis
from redis.exceptions import ResponseError
from sqlalchemy import or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import DataError

from analytics.db import get_session_factory
from analytics.engine_event_models import EngineEvent
from analytics.opportunity_store import OpportunityStore
from api.settings import settings

logger = logging.getLogger(__name__)

_CONSUMER_METRICS = {
    "dead_letter_events_total": 0,
    "database_duplicate_events_total": 0,
}


def consumer_metrics_snapshot() -> dict[str, int]:
    return dict(_CONSUMER_METRICS)


@dataclass(frozen=True)
class StreamEvent:
    stream_id: str
    event_id: str
    event_type: str
    source: str
    schema_version: int
    occurred_at_ms: int
    payload: dict


class EngineEventConsumer:
    def __init__(self) -> None:
        self._stopping = asyncio.Event()
        host = socket.gethostname().replace(":", "-")
        self._consumer = f"{host}-{os.getpid()}"

    async def stop(self) -> None:
        self._stopping.set()

    async def run(self) -> None:
        while not self._stopping.is_set():
            try:
                await self._consume_session()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("engine event consumer session failed")
                await asyncio.sleep(settings.arb_event_retry_seconds)

    async def _consume_session(self) -> None:
        client = redis.from_url(
            settings.arb_redis_url,
            decode_responses=True,
        )
        try:
            try:
                await client.xgroup_create(
                    settings.arb_event_stream,
                    settings.arb_event_consumer_group,
                    id="0-0",
                    mkstream=True,
                )
            except ResponseError as error:
                if "BUSYGROUP" not in str(error):
                    raise

            claim_cursor = "0-0"
            while not self._stopping.is_set():
                claimed = await client.execute_command(
                    "XAUTOCLAIM",
                    settings.arb_event_stream,
                    settings.arb_event_consumer_group,
                    self._consumer,
                    30_000,
                    claim_cursor,
                    "COUNT",
                    settings.arb_event_batch_size,
                )
                next_cursor = (
                    str(claimed[0])
                    if claimed and claimed[0]
                    else "0-0"
                )
                claimed_messages = claimed[1] if len(claimed) > 1 else []
                claim_cursor = next_cursor

                if claimed_messages:
                    await self._persist_and_ack(client, claimed_messages)
                    continue

                if claim_cursor != "0-0":
                    continue

                batches = await client.xreadgroup(
                    groupname=settings.arb_event_consumer_group,
                    consumername=self._consumer,
                    streams={settings.arb_event_stream: ">"},
                    count=settings.arb_event_batch_size,
                    block=5000,
                )
                if not batches:
                    continue

                for _stream, messages in batches:
                    await self._persist_and_ack(client, messages)
        finally:
            await client.aclose()

    async def _persist_and_ack(self, client, messages) -> None:
        events: list[StreamEvent] = []
        invalid_ids: list[str] = []

        for stream_id, fields in messages:
            try:
                events.append(_parse(stream_id, fields))
            except (KeyError, TypeError, ValueError) as error:
                await client.xadd(
                    settings.arb_event_dead_letter_stream,
                    {
                        "original_stream_id": stream_id,
                        "retry_count": "1",
                        "error": str(error),
                        "payload": json.dumps(fields, separators=(",", ":")),
                    },
                )
                invalid_ids.append(stream_id)
                _CONSUMER_METRICS["dead_letter_events_total"] += 1
                logger.error(
                    "invalid engine event moved to dead-letter stream: %s",
                    stream_id,
                )

        if invalid_ids:
            await client.xack(
                settings.arb_event_stream,
                settings.arb_event_consumer_group,
                *invalid_ids,
            )

        if events:
            rejected = await asyncio.to_thread(_persist, events)
            for event, error in rejected:
                await client.xadd(
                    settings.arb_event_dead_letter_stream,
                    {
                        "original_stream_id": event.stream_id,
                        "retry_count": "1",
                        "error": f"database rejected event: {error}"[:1000],
                        "payload": json.dumps(
                            {
                                "event_id": event.event_id,
                                "event_type": event.event_type,
                                "source": event.source,
                                "schema_version": event.schema_version,
                                "occurred_at_ms": event.occurred_at_ms,
                                "payload": event.payload,
                            },
                            separators=(",", ":"),
                        ),
                    },
                )
                _CONSUMER_METRICS["dead_letter_events_total"] += 1
                logger.error(
                    "database-rejected engine event moved to dead-letter stream: %s",
                    event.stream_id,
                )

            await client.xack(
                settings.arb_event_stream,
                settings.arb_event_consumer_group,
                *[event.stream_id for event in events],
            )


MAX_SIGNED_BIGINT = 9_223_372_036_854_775_807
MAX_IDENTITY_LENGTH = 64


def _parse(stream_id: str, fields: dict[str, str]) -> StreamEvent:
    stream_id = stream_id.strip()
    event_id = fields["event_id"].strip()
    event_type = fields["event_type"].strip()
    source = fields["source"].strip()

    for name, value in (
        ("stream_id", stream_id),
        ("event_id", event_id),
        ("event_type", event_type),
        ("source", source),
    ):
        if not value:
            raise ValueError(f"{name} must not be empty")
        if len(value) > MAX_IDENTITY_LENGTH:
            raise ValueError(
                f"{name} exceeds {MAX_IDENTITY_LENGTH} characters"
            )

    schema_version = int(fields.get("schema_version", "1"))
    if schema_version != 1:
        raise ValueError(
            f"unsupported engine event schema version {schema_version}"
        )

    occurred_at_ms = int(fields["occurred_at_ms"])
    if occurred_at_ms < 0 or occurred_at_ms > MAX_SIGNED_BIGINT:
        raise ValueError("occurred_at_ms is outside PostgreSQL BIGINT range")

    payload = json.loads(
        fields.get("payload", "{}"),
        parse_constant=_reject_json_constant,
    )
    if not isinstance(payload, dict):
        raise ValueError("engine event payload must be a JSON object")

    return StreamEvent(
        stream_id=stream_id,
        event_id=event_id,
        event_type=event_type,
        source=source,
        schema_version=schema_version,
        occurred_at_ms=occurred_at_ms,
        payload=payload,
    )


def _reject_json_constant(value: str):
    raise ValueError(f"non-finite JSON value is not allowed: {value}")


def _persist(
    events: list[StreamEvent],
) -> list[tuple[StreamEvent, str]]:
    rejected: list[tuple[StreamEvent, str]] = []
    with get_session_factory()() as session:
        opportunity_store = OpportunityStore(
            session,
            min_net_edge_bps=Decimal(str(settings.arb_opportunity_min_net_bps)),
            max_continuity_gap_ms=settings.arb_opportunity_max_gap_ms,
        )
        for event in events:
            statement = (
                insert(EngineEvent)
                .values(
                    event_id=event.event_id,
                    stream_id=event.stream_id,
                    event_type=event.event_type,
                    source=event.source,
                    schema_version=event.schema_version,
                    occurred_at_ms=event.occurred_at_ms,
                    payload=event.payload,
                )
                .on_conflict_do_nothing()
            )
            try:
                with session.begin_nested():
                    result = session.execute(statement)
                    if result.rowcount == 0:
                        # ON CONFLICT covers both the event ID and Redis stream ID.
                        # Only an identical event may be treated as an idempotent replay.
                        # Otherwise a conflicting financial/event record would disappear.
                        existing = session.scalar(
                            select(EngineEvent).where(
                                or_(
                                    EngineEvent.event_id == event.event_id,
                                    EngineEvent.stream_id == event.stream_id,
                                )
                            ).limit(1)
                        )
                        if (
                            existing is None
                            or existing.event_id != event.event_id
                            or existing.event_type != event.event_type
                            or existing.source != event.source
                            or existing.schema_version != event.schema_version
                            or existing.occurred_at_ms != event.occurred_at_ms
                            or existing.payload != event.payload
                        ):
                            raise ValueError(
                                "conflicting event_id or stream_id; event requires investigation"
                            )
                        _CONSUMER_METRICS[
                            "database_duplicate_events_total"
                        ] += 1
                    elif (
                        event.source == "engine-service"
                        and event.event_type in {"opportunity.detected", "opportunity.rejected"}
                        and "candidate_id" in event.payload
                    ):
                        # One transaction commits the event and its dashboard observation.
                        # On retry, the event's unique ID prevents duplicate accounting.
                        opportunity_store.record_scan(event.payload)
            except (DataError, KeyError, TypeError, ValueError, OverflowError) as error:
                rejected.append((event, str(error)))
        session.commit()
    return rejected
