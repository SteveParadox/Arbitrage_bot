from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from typing import Iterable

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from simulator.models import MarketBookEvent


@dataclass(frozen=True)
class ReplayEvent:
    symbol: str
    timestamp_ms: int
    update_id: int
    sequence: int
    is_snapshot: bool
    bids: tuple[tuple[float, float], ...]
    asks: tuple[tuple[float, float], ...]


@dataclass(frozen=True)
class BookState:
    symbol: str
    timestamp_ms: int
    update_id: int
    sequence: int
    bids: tuple[tuple[float, float], ...]
    asks: tuple[tuple[float, float], ...]

    @property
    def best_bid(self) -> float | None:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return self.asks[0][0] if self.asks else None


@dataclass(frozen=True)
class _Checkpoint:
    index: int
    bids: tuple[tuple[float, float], ...]
    asks: tuple[tuple[float, float], ...]


class SymbolReplay:
    def __init__(
        self,
        symbol: str,
        events: list[ReplayEvent],
        *,
        checkpoint_interval: int = 100,
    ) -> None:
        if checkpoint_interval <= 0:
            raise ValueError("checkpoint_interval must be greater than zero")
        self.symbol = symbol
        self.events = sorted(events, key=lambda item: (item.timestamp_ms, item.sequence))
        self.timestamps = [event.timestamp_ms for event in self.events]
        self.checkpoint_interval = checkpoint_interval
        self._checkpoints: list[_Checkpoint] = []
        self._checkpoint_indexes: list[int] = []
        self._build_checkpoints()

    def _build_checkpoints(self) -> None:
        bids: dict[float, float] = {}
        asks: dict[float, float] = {}
        initialized = False

        for index, event in enumerate(self.events):
            if event.is_snapshot:
                bids.clear()
                asks.clear()
                initialized = True
            elif not initialized:
                continue

            _apply_levels(bids, event.bids)
            _apply_levels(asks, event.asks)

            if event.is_snapshot or index % self.checkpoint_interval == 0:
                checkpoint = _Checkpoint(
                    index=index,
                    bids=tuple(bids.items()),
                    asks=tuple(asks.items()),
                )
                self._checkpoints.append(checkpoint)
                self._checkpoint_indexes.append(index)

    def state_at(self, timestamp_ms: int) -> BookState | None:
        event_index = bisect_right(self.timestamps, timestamp_ms) - 1
        if event_index < 0 or not self._checkpoints:
            return None

        checkpoint_position = bisect_right(self._checkpoint_indexes, event_index) - 1
        if checkpoint_position < 0:
            return None

        checkpoint = self._checkpoints[checkpoint_position]
        bids = dict(checkpoint.bids)
        asks = dict(checkpoint.asks)

        for event in self.events[checkpoint.index + 1 : event_index + 1]:
            if event.is_snapshot:
                bids.clear()
                asks.clear()
            _apply_levels(bids, event.bids)
            _apply_levels(asks, event.asks)

        event = self.events[event_index]
        return BookState(
            symbol=self.symbol,
            timestamp_ms=event.timestamp_ms,
            update_id=event.update_id,
            sequence=event.sequence,
            bids=tuple(sorted(bids.items(), key=lambda item: item[0], reverse=True)),
            asks=tuple(sorted(asks.items(), key=lambda item: item[0])),
        )


def _apply_levels(
    side: dict[float, float],
    levels: Iterable[tuple[float, float]],
) -> None:
    for price, quantity in levels:
        if quantity == 0:
            side.pop(price, None)
        else:
            side[price] = quantity


def _decode_levels(levels: list) -> tuple[tuple[float, float], ...]:
    decoded: list[tuple[float, float]] = []
    for level in levels:
        if isinstance(level, dict):
            decoded.append((float(level["price"]), float(level["quantity"])))
        else:
            decoded.append((float(level[0]), float(level[1])))
    return tuple(decoded)


def load_symbol_replays(
    session: Session,
    symbols: set[str],
    *,
    start_ms: int,
    end_ms: int,
    checkpoint_interval: int = 100,
) -> dict[str, SymbolReplay]:
    replays: dict[str, SymbolReplay] = {}

    for symbol in sorted(symbols):
        snapshot = session.scalar(
            select(MarketBookEvent)
            .where(
                MarketBookEvent.symbol == symbol,
                MarketBookEvent.is_snapshot.is_(True),
                MarketBookEvent.event_timestamp_ms <= start_ms,
            )
            .order_by(
                MarketBookEvent.event_timestamp_ms.desc(),
                MarketBookEvent.sequence.desc(),
            )
            .limit(1)
        )

        lower_bound = snapshot.event_timestamp_ms if snapshot is not None else start_ms
        rows = session.scalars(
            select(MarketBookEvent)
            .where(
                and_(
                    MarketBookEvent.symbol == symbol,
                    MarketBookEvent.event_timestamp_ms >= lower_bound,
                    MarketBookEvent.event_timestamp_ms <= end_ms,
                )
            )
            .order_by(MarketBookEvent.event_timestamp_ms, MarketBookEvent.sequence)
        ).all()

        events = [
            ReplayEvent(
                symbol=row.symbol,
                timestamp_ms=row.event_timestamp_ms,
                update_id=row.update_id,
                sequence=row.sequence,
                is_snapshot=row.is_snapshot,
                bids=_decode_levels(row.bids),
                asks=_decode_levels(row.asks),
            )
            for row in rows
        ]
        if events:
            replays[symbol] = SymbolReplay(
                symbol,
                events,
                checkpoint_interval=checkpoint_interval,
            )

    return replays
