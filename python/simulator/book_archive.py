from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from math import isfinite
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
        # Stable archive arrival order breaks ties; sequence can reset within a millisecond.
        self.events = sorted(events, key=lambda item: item.timestamp_ms)
        self.timestamps = [event.timestamp_ms for event in self.events]
        self.checkpoint_interval = checkpoint_interval
        self._checkpoints: list[_Checkpoint] = []
        self._checkpoint_indexes: list[int] = []
        self._valid: list[bool] = []
        self.failure_reason: str | None = None
        self._build_checkpoints()

    def _build_checkpoints(self) -> None:
        bids: dict[float, float] = {}
        asks: dict[float, float] = {}
        initialized = False
        previous: ReplayEvent | None = None

        for index, event in enumerate(self.events):
            snapshot = event.is_snapshot or event.update_id == 1
            valid = (
                event.symbol == self.symbol
                and event.timestamp_ms >= 0 and event.update_id > 0 and event.sequence >= 0
                and valid_levels(event.bids) and valid_levels(event.asks)
            )
            if not snapshot and previous is not None:
                valid = valid and event.sequence > previous.sequence
                valid = valid and event.update_id > previous.update_id
            if not valid:
                initialized = False
            if valid and snapshot:
                bids.clear()
                asks.clear()
                initialized = True
            self._valid.append(valid and initialized)
            if not valid or not initialized:
                continue

            _apply_levels(bids, event.bids)
            _apply_levels(asks, event.asks)
            previous = event

            if snapshot or index % self.checkpoint_interval == 0:
                checkpoint = _Checkpoint(
                    index=index,
                    bids=tuple(bids.items()),
                    asks=tuple(asks.items()),
                )
                self._checkpoints.append(checkpoint)
                self._checkpoint_indexes.append(index)

    def state_at(self, timestamp_ms: int) -> BookState | None:
        event_index = bisect_right(self.timestamps, timestamp_ms) - 1
        if event_index < 0 or not self._checkpoints or not self._valid[event_index]:
            return None

        checkpoint_position = bisect_right(self._checkpoint_indexes, event_index) - 1
        if checkpoint_position < 0:
            return None

        checkpoint = self._checkpoints[checkpoint_position]
        bids = dict(checkpoint.bids)
        asks = dict(checkpoint.asks)

        for event in self.events[checkpoint.index + 1 : event_index + 1]:
            if event.is_snapshot or event.update_id == 1:
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


def valid_levels(levels: Iterable[tuple[float, float]]) -> bool:
    return all(isfinite(p) and p > 0 and isfinite(q) and q >= 0 for p, q in levels)


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
    result = tuple(decoded)
    if not valid_levels(result):
        raise ValueError("book levels must have finite positive prices and nonnegative sizes")
    return result


def load_symbol_replays(
    session: Session,
    symbols: set[str],
    *,
    start_ms: int,
    end_ms: int,
    checkpoint_interval: int = 100,
    max_history_events: int = 100_000,
) -> dict[str, SymbolReplay]:
    replays: dict[str, SymbolReplay] = {}
    if max_history_events <= 0:
        raise ValueError("max_history_events must be positive")
    remaining_budget = max_history_events

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
                MarketBookEvent.id.desc(),
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
            .order_by(MarketBookEvent.event_timestamp_ms, MarketBookEvent.id)
            .limit(remaining_budget + 1)
        ).all()

        if len(rows) > remaining_budget:
            unavailable = SymbolReplay(symbol, [])
            unavailable.failure_reason = "history_limit_exceeded"
            replays[symbol] = unavailable
            continue
        remaining_budget -= len(rows)

        try:
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
        except (ValueError, TypeError, KeyError, IndexError, OverflowError):
            unavailable = SymbolReplay(symbol, [])
            unavailable.failure_reason = "invalid_book_history"
            replays[symbol] = unavailable
            continue
        if events:
            replays[symbol] = SymbolReplay(
                symbol,
                events,
                checkpoint_interval=checkpoint_interval,
            )

    return replays
