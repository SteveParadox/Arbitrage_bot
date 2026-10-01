from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
from dataclasses import dataclass

import redis.asyncio as redis
from redis.exceptions import ResponseError
from sqlalchemy.dialects.postgresql import insert

from analytics.db import get_session_factory
from analytics.engine_event_models import EngineEvent
from api.settings import settings

logger = logging.getLogger(__name__)


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

            while not self._stopping.is_set():
                claimed = await client.execute_command(
                    "XAUTOCLAIM",
                    settings.arb_event_stream,
                    settings.arb_event_consumer_group,
                    self._consumer,
                    30_000,
                    "0-0",
                    "COUNT",
                    settings.arb_event_batch_size,
                )
                claimed_messages = claimed[1] if len(claimed) > 1 else []
                if claimed_messages:
                    await self._persist_and_ack(client, claimed_messages)
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
                        "error": str(error),
                        "payload": json.dumps(fields, separators=(",", ":")),
                    },
                )
                invalid_ids.append(stream_id)
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
            await asyncio.to_thread(_persist, events)
            await client.xack(
                settings.arb_event_stream,
                settings.arb_event_consumer_group,
                *[event.stream_id for event in events],
            )


def _parse(stream_id: str, fields: dict[str, str]) -> StreamEvent:
    event_id = fields["event_id"].strip()
    event_type = fields["event_type"].strip()
    source = fields["source"].strip()
    if not event_id or not event_type or not source:
        raise ValueError("engine event identity fields must not be empty")

    schema_version = int(fields.get("schema_version", "1"))
    if schema_version != 1:
        raise ValueError(
            f"unsupported engine event schema version {schema_version}"
        )

    payload = json.loads(fields.get("payload", "{}"))
    if not isinstance(payload, dict):
        raise ValueError("engine event payload must be a JSON object")

    return StreamEvent(
        stream_id=stream_id,
        event_id=event_id,
        event_type=event_type,
        source=source,
        schema_version=schema_version,
        occurred_at_ms=int(fields["occurred_at_ms"]),
        payload=payload,
    )


def _persist(events: list[StreamEvent]) -> None:
    with get_session_factory()() as session:
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
                .on_conflict_do_nothing(index_elements=["event_id"])
            )
            session.execute(statement)
        session.commit()
