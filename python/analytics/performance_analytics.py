from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from statistics import fmean
from typing import Any, Iterable

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from analytics.engine_event_models import EngineEvent
from analytics.micro_live_models import MicroLiveCycle
from analytics.models import OpportunityWindow
from api.settings import settings


@dataclass(frozen=True)
class ActualCycle:
    trade_id: str
    opportunity_window_id: str | None
    occurred_at: datetime
    base_asset: str
    starting_amount: Decimal
    pnl: Decimal | None
    execution_time_ms: int | None
    estimated_turnover: Decimal
    source: str
    successful: bool | None


def build_performance_analytics(
    db: Session,
    *,
    days: int = 7,
    base_asset: str = "USDT",
    bins: int = 10,
) -> dict[str, Any]:
    now = datetime.now(UTC)
    start = now - timedelta(days=days)
    start_ms = int(start.timestamp() * 1000)
    base_asset = base_asset.upper()
    sample_limit = min(
        max(settings.arb_performance_sample_limit, 1_000),
        1_000_000,
    )

    windows = _opportunity_windows(db, start, base_asset, sample_limit)
    observed_count = db.scalar(
        select(func.count(OpportunityWindow.id)).where(
            OpportunityWindow.started_at >= start,
            OpportunityWindow.start_asset == base_asset,
        )
    ) or 0
    closed_window_count = db.scalar(
        select(func.count(OpportunityWindow.id)).where(
            OpportunityWindow.started_at >= start,
            OpportunityWindow.start_asset == base_asset,
            OpportunityWindow.ended_at.is_not(None),
        )
    ) or 0
    open_window_count = db.scalar(
        select(func.count(OpportunityWindow.id)).where(
            OpportunityWindow.started_at >= start,
            OpportunityWindow.start_asset == base_asset,
            OpportunityWindow.ended_at.is_(None),
        )
    ) or 0

    expected_count = db.scalar(
        select(func.count(OpportunityWindow.id)).where(
            OpportunityWindow.started_at >= start,
            OpportunityWindow.start_asset == base_asset,
            OpportunityWindow.max_net_profit.is_not(None),
            OpportunityWindow.max_net_profit > 0,
        )
    ) or 0
    expected_profit_total = db.scalar(
        select(func.coalesce(func.sum(OpportunityWindow.max_net_profit), 0)).where(
            OpportunityWindow.started_at >= start,
            OpportunityWindow.start_asset == base_asset,
            OpportunityWindow.max_net_profit > 0,
        )
    ) or Decimal.ZERO

    engine_event_total = db.scalar(
        select(func.count(EngineEvent.event_id)).where(
            EngineEvent.occurred_at_ms >= start_ms,
            EngineEvent.event_type.in_(
                [
                    "trade.attempted",
                    "trade.executed",
                    "trade.failed",
                    "order.executed",
                    "order.failed",
                ]
            ),
        )
    ) or 0
    canary_cycle_total = db.scalar(
        select(func.count(MicroLiveCycle.trade_id)).where(
            MicroLiveCycle.reconciled_at >= start,
            MicroLiveCycle.base_asset == base_asset,
        )
    ) or 0

    (
        engine_cycles,
        attempted_engine_ids,
        orphan_order_attempts,
        attempted_event_count,
        engine_events_loaded,
        turnover_fallback_count,
    ) = _engine_cycles(
        db,
        start_ms=start_ms,
        base_asset=base_asset,
        limit=sample_limit,
    )
    canary_cycles = _canary_cycles(
        db,
        start=start,
        base_asset=base_asset,
        limit=sample_limit,
    )

    engine_ids = {cycle.trade_id for cycle in engine_cycles}
    unique_canary_cycles = [
        cycle for cycle in canary_cycles if cycle.trade_id not in engine_ids
    ]
    by_trade_id: dict[str, ActualCycle] = {
        cycle.trade_id: cycle for cycle in engine_cycles
    }
    for cycle in unique_canary_cycles:
        by_trade_id[cycle.trade_id] = cycle

    actual_cycles = sorted(
        by_trade_id.values(),
        key=lambda cycle: cycle.occurred_at,
    )
    matched_capture = _matched_opportunity_capture(
        db,
        actual_cycles,
        start=start,
        base_asset=base_asset,
    )
    attempted_ids = set(attempted_engine_ids)
    attempted_ids.update(
        cycle.trade_id for cycle in unique_canary_cycles
    )

    known_profit_cycles = [
        cycle for cycle in actual_cycles if cycle.pnl is not None
    ]
    total_actual_profit = sum(
        (cycle.pnl for cycle in known_profit_cycles if cycle.pnl is not None),
        Decimal.ZERO,
    )
    total_turnover = sum(
        (cycle.estimated_turnover for cycle in known_profit_cycles),
        Decimal.ZERO,
    )

    daily = _profit_per_day(
        known_profit_cycles,
        start=start,
        end=now,
    )
    pnl_values = [float(cycle.pnl) for cycle in known_profit_cycles if cycle.pnl is not None]
    latency_values = [
        float(cycle.execution_time_ms)
        for cycle in actual_cycles
        if cycle.execution_time_ms is not None
    ]

    net_edge_total = db.scalar(
        select(func.count(OpportunityWindow.id)).where(
            OpportunityWindow.started_at >= start,
            OpportunityWindow.start_asset == base_asset,
            OpportunityWindow.max_net_edge_bps.is_not(None),
        )
    ) or 0
    net_edges = [
        float(window.max_net_edge_bps)
        for window in windows
        if window.max_net_edge_bps is not None
    ]

    slippage_total = db.scalar(
        select(func.count(MicroLiveCycle.trade_id)).where(
            MicroLiveCycle.reconciled_at >= start,
            MicroLiveCycle.base_asset == base_asset,
            MicroLiveCycle.actual_slippage_bps.is_not(None),
        )
    ) or 0
    slippage = [
        float(value)
        for value in db.scalars(
            select(MicroLiveCycle.actual_slippage_bps)
            .where(
                MicroLiveCycle.reconciled_at >= start,
                MicroLiveCycle.base_asset == base_asset,
                MicroLiveCycle.actual_slippage_bps.is_not(None),
            )
            .order_by(desc(MicroLiveCycle.reconciled_at))
            .limit(sample_limit)
        ).all()
        if value is not None
    ]

    closed_survival = [
        float(window.duration_ms)
        for window in windows
        if window.ended_at is not None
    ]
    wins = sum(1 for value in pnl_values if value > 0)
    losses = sum(1 for value in pnl_values if value < 0)
    breakeven = sum(1 for value in pnl_values if value == 0)
    resolved = wins + losses + breakeven

    engine_known = [cycle for cycle in engine_cycles if cycle.pnl is not None]
    canary_known = [
        cycle for cycle in unique_canary_cycles if cycle.pnl is not None
    ]
    engine_profit = sum(
        (cycle.pnl for cycle in engine_known if cycle.pnl is not None),
        Decimal.ZERO,
    )
    canary_profit = sum(
        (cycle.pnl for cycle in canary_known if cycle.pnl is not None),
        Decimal.ZERO,
    )

    return {
        "generated_at": now,
        "window": {
            "days": days,
            "start": start,
            "end": now,
            "base_asset": base_asset,
        },
        "profit": {
            "total_profit": _float(total_actual_profit),
            "cycles_with_known_pnl": len(known_profit_cycles),
            "profit_per_cycle": (
                _float(total_actual_profit / len(known_profit_cycles))
                if known_profit_cycles
                else None
            ),
            "average_profit_per_day": _float(
                total_actual_profit / Decimal(days)
            ),
            "estimated_turnover": _float(total_turnover),
            "profit_per_1000_turnover": (
                _float(total_actual_profit / total_turnover * Decimal(1000))
                if total_turnover > 0
                else None
            ),
            "turnover_basis": (
                "estimated: new engine events use base-asset execution flows plus a "
                "fill-ratio proxy for cross legs and include unwind orders; older "
                "engine events fall back to starting capital × (legs + unwinds); "
                "manual canary cycles assume three legs"
            ),
            "daily": daily,
            "by_source": {
                "engine": {
                    "cycles": len(engine_known),
                    "profit": _float(engine_profit),
                },
                "micro_canary": {
                    "cycles": len(canary_known),
                    "profit": _float(canary_profit),
                },
            },
        },
        "funnel": {
            "observed_opportunities": int(observed_count),
            "expected_profitable_opportunities": int(expected_count),
            "expected_profit_total": _float(expected_profit_total),
            "expected_profit_basis": (
                "sum of max expected net profit per Phase 7 opportunity window"
            ),
            "trade_attempted": len(attempted_ids),
            "trade_attempted_engine": len(attempted_engine_ids),
            "trade_attempted_micro_canary": len(unique_canary_cycles),
            "actual_profit_known": len(known_profit_cycles),
            "actual_profit_total": _float(total_actual_profit),
            "attempt_to_actual_known_pct": (
                len(known_profit_cycles) / len(attempted_ids) * 100.0
                if attempted_ids
                else None
            ),
            "aggregate_profit_capture_pct": (
                _float(
                    total_actual_profit
                    / Decimal(str(expected_profit_total))
                    * Decimal(100)
                )
                if Decimal(str(expected_profit_total)) > 0
                else None
            ),
            **matched_capture,
            "population_note": (
                "opportunity stages are distinct Phase 7 windows; trade stages "
                "are distinct execution trade ids over the same time window, "
                "so aggregate conversion ratios are diagnostic rather than "
                "a one-to-one attribution"
            ),
        },
        "distributions": {
            "net_edge_bps": {
                **_distribution(
                    net_edges,
                    bins=bins,
                    total_count=int(net_edge_total),
                    unit="bps",
                ),
                "basis": (
                    "maximum modeled net edge per distinct Phase 7 "
                    "opportunity window"
                ),
            },
            "win_loss": {
                "wins": wins,
                "losses": losses,
                "breakeven": breakeven,
                "unknown_pnl": max(len(attempted_ids) - resolved, 0),
                "win_rate_pct": (
                    wins / resolved * 100.0 if resolved else None
                ),
            },
            "opportunity_survival_ms": {
                **_distribution(
                    closed_survival,
                    bins=bins,
                    total_count=int(closed_window_count),
                    unit="ms",
                ),
                "open_windows": int(open_window_count),
                "basis": "closed Phase 7 opportunity windows",
            },
            "latency_ms": {
                **_distribution(
                    latency_values,
                    bins=bins,
                    total_count=len(latency_values),
                    unit="ms",
                ),
                "basis": (
                    "engine terminal execution duration when available; "
                    "otherwise reconciled micro-canary execution time"
                ),
            },
            "slippage_bps": {
                **_distribution(
                    slippage,
                    bins=bins,
                    total_count=int(slippage_total),
                    unit="bps",
                ),
                "basis": "reconciled Phase 13 actual slippage",
            },
        },
        "data_quality": {
            "sample_limit": sample_limit,
            "opportunity_windows_sampled": len(windows) < int(observed_count),
            "net_edge_sampled": len(net_edges) < int(net_edge_total),
            "slippage_sampled": len(slippage) < int(slippage_total),
            "engine_terminal_events": len(engine_cycles),
            "engine_trade_attempt_events": attempted_event_count,
            "engine_events_loaded": engine_events_loaded,
            "engine_events_total": int(engine_event_total),
            "engine_events_sampled": (
                engine_events_loaded < int(engine_event_total)
            ),
            "engine_turnover_fallbacks": turnover_fallback_count,
            "engine_attributed_terminal_events": sum(
                1
                for cycle in engine_cycles
                if cycle.opportunity_window_id is not None
            ),
            "micro_canary_cycles": len(unique_canary_cycles),
            "micro_canary_cycles_loaded": len(canary_cycles),
            "micro_canary_cycles_total": int(canary_cycle_total),
            "micro_canary_cycles_sampled": (
                len(canary_cycles) < int(canary_cycle_total)
            ),
            "profit_metrics_sampled": (
                engine_events_loaded < int(engine_event_total)
                or len(canary_cycles) < int(canary_cycle_total)
            ),
            "deduplicated_canary_trade_ids": (
                len(canary_cycles) - len(unique_canary_cycles)
            ),
            "orphan_order_attempts_without_terminal_trade": orphan_order_attempts,
            "note": (
                "new engine data uses explicit trade.attempted events; older "
                "history falls back to terminal route events. Opportunity "
                "stages and trade stages remain distinct populations over "
                "the same selected time window."
            ),
        },
    }


def _opportunity_windows(
    db: Session,
    start: datetime,
    base_asset: str,
    limit: int,
) -> list[OpportunityWindow]:
    return list(
        db.scalars(
            select(OpportunityWindow)
            .where(
                OpportunityWindow.started_at >= start,
                OpportunityWindow.start_asset == base_asset,
            )
            .order_by(desc(OpportunityWindow.started_at))
            .limit(limit)
        ).all()
    )


def _engine_cycles(
    db: Session,
    *,
    start_ms: int,
    base_asset: str,
    limit: int,
) -> tuple[list[ActualCycle], set[str], int, int, int, int]:
    rows = db.scalars(
        select(EngineEvent)
        .where(
            EngineEvent.occurred_at_ms >= start_ms,
            EngineEvent.event_type.in_(
                [
                    "trade.attempted",
                    "trade.executed",
                    "trade.failed",
                    "order.executed",
                    "order.failed",
                ]
            ),
        )
        .order_by(desc(EngineEvent.occurred_at_ms))
        .limit(limit)
    ).all()

    order_trade_ids: set[str] = set()
    terminal_all_ids: set[str] = set()
    attempted_all_ids: set[str] = set()
    attempted_engine_ids: set[str] = set()
    attempted_event_count = 0
    turnover_fallback_count = 0
    terminal: dict[str, ActualCycle] = {}

    for event in reversed(rows):
        trade_id = str(event.payload.get("trade_id") or "").strip()
        if not trade_id:
            continue
        if event.event_type in {"order.executed", "order.failed"}:
            order_trade_ids.add(trade_id)
            continue
        if event.event_type == "trade.attempted":
            attempted_all_ids.add(trade_id)
            if str(event.payload.get("base_asset") or "").upper() == base_asset:
                attempted_engine_ids.add(trade_id)
                attempted_event_count += 1
            continue

        terminal_all_ids.add(trade_id)
        event_asset = str(event.payload.get("base_asset") or "").upper()
        if event_asset != base_asset:
            continue

        starting = _decimal(event.payload.get("starting_amount"))
        pnl = _decimal(
            event.payload.get("economic_pnl")
            if event.payload.get("economic_pnl") is not None
            else event.payload.get("realized_base_pnl")
        )
        leg_count = _int(event.payload.get("leg_count"))
        estimated_turnover, used_turnover_fallback = _engine_turnover(
            event.payload,
            starting=starting,
            leg_count=leg_count,
        )
        if used_turnover_fallback:
            turnover_fallback_count += 1
        attempted_engine_ids.add(trade_id)
        terminal[trade_id] = ActualCycle(
            trade_id=trade_id,
            opportunity_window_id=_text_or_none(
                event.payload.get("opportunity_window_id")
            ),
            occurred_at=datetime.fromtimestamp(
                event.occurred_at_ms / 1000,
                tz=UTC,
            ),
            base_asset=event_asset,
            starting_amount=starting or Decimal.ZERO,
            pnl=pnl,
            execution_time_ms=_int_or_none(
                event.payload.get("execution_time_ms")
            ),
            estimated_turnover=estimated_turnover,
            source="engine",
            successful=event.event_type == "trade.executed",
        )

    orphan_order_attempts = len(
        order_trade_ids - terminal_all_ids - attempted_all_ids
    )
    return (
        list(terminal.values()),
        attempted_engine_ids,
        orphan_order_attempts,
        attempted_event_count,
        len(rows),
        turnover_fallback_count,
    )


def _engine_turnover(
    payload: dict[str, Any],
    *,
    starting: Decimal | None,
    leg_count: int,
) -> tuple[Decimal, bool]:
    explicit = _decimal(payload.get("estimated_turnover_base"))
    if explicit is not None and explicit > 0:
        return abs(explicit), False

    unwind_count = _int(payload.get("unwind_count"))
    order_count = leg_count + unwind_count
    if starting is None or order_count <= 0:
        return Decimal.ZERO, True

    return abs(starting) * order_count, True


def _canary_cycles(
    db: Session,
    *,
    start: datetime,
    base_asset: str,
    limit: int,
) -> list[ActualCycle]:
    rows = db.scalars(
        select(MicroLiveCycle)
        .where(
            MicroLiveCycle.reconciled_at >= start,
            MicroLiveCycle.base_asset == base_asset,
        )
        .order_by(desc(MicroLiveCycle.reconciled_at))
        .limit(limit)
    ).all()

    cycles: list[ActualCycle] = []
    for row in rows:
        starting = Decimal(row.starting_capital)
        pnl = Decimal(row.realized_pnl) if row.realized_pnl is not None else None
        cycles.append(
            ActualCycle(
                trade_id=row.trade_id,
                opportunity_window_id=_text_or_none(
                    row.raw_candidate_event.get("opportunity_window_id")
                ),
                occurred_at=row.reconciled_at or row.detected_at,
                base_asset=row.base_asset,
                starting_amount=starting,
                pnl=pnl,
                execution_time_ms=row.execution_time_ms,
                estimated_turnover=abs(starting) * Decimal(3),
                source="micro_canary",
                successful=(pnl >= 0 if pnl is not None else None),
            )
        )
    return cycles


def _matched_opportunity_capture(
    db: Session,
    cycles: Iterable[ActualCycle],
    *,
    start: datetime,
    base_asset: str,
) -> dict[str, Any]:
    cycles_by_window: dict[str, list[ActualCycle]] = defaultdict(list)
    for cycle in cycles:
        if cycle.opportunity_window_id is None or cycle.pnl is None:
            continue
        cycles_by_window[cycle.opportunity_window_id].append(cycle)

    if not cycles_by_window:
        return {
            "matched_opportunity_windows": 0,
            "matched_actual_cycles": 0,
            "matched_expected_profit_total": 0.0,
            "matched_actual_profit_total": 0.0,
            "matched_profit_capture_pct": None,
        }

    windows = db.scalars(
        select(OpportunityWindow).where(
            OpportunityWindow.id.in_(cycles_by_window),
            OpportunityWindow.started_at >= start,
            OpportunityWindow.start_asset == base_asset,
            OpportunityWindow.max_net_profit.is_not(None),
            OpportunityWindow.max_net_profit > 0,
        )
    ).all()

    expected_by_window = {
        window.id: Decimal(window.max_net_profit)
        for window in windows
        if window.max_net_profit is not None
    }
    matched_ids = set(expected_by_window)
    matched_cycles = [
        cycle
        for window_id in matched_ids
        for cycle in cycles_by_window[window_id]
    ]
    expected_total = sum(
        expected_by_window.values(),
        Decimal.ZERO,
    )
    actual_total = sum(
        (
            cycle.pnl
            for cycle in matched_cycles
            if cycle.pnl is not None
        ),
        Decimal.ZERO,
    )

    return {
        "matched_opportunity_windows": len(matched_ids),
        "matched_actual_cycles": len(matched_cycles),
        "matched_expected_profit_total": _float(expected_total),
        "matched_actual_profit_total": _float(actual_total),
        "matched_profit_capture_pct": (
            _float(actual_total / expected_total * Decimal(100))
            if expected_total > 0
            else None
        ),
    }


def _profit_per_day(
    cycles: Iterable[ActualCycle],
    *,
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
    by_day: dict[date, dict[str, Any]] = defaultdict(
        lambda: {"profit": Decimal.ZERO, "cycles": 0}
    )
    for cycle in cycles:
        if cycle.pnl is None:
            continue
        day = cycle.occurred_at.astimezone(UTC).date()
        by_day[day]["profit"] += cycle.pnl
        by_day[day]["cycles"] += 1

    rows: list[dict[str, Any]] = []
    current = start.astimezone(UTC).date()
    final = end.astimezone(UTC).date()
    while current <= final:
        values = by_day[current]
        rows.append(
            {
                "date": current.isoformat(),
                "profit": _float(values["profit"]),
                "cycles": values["cycles"],
            }
        )
        current += timedelta(days=1)
    return rows

def _distribution(
    values: list[float],
    *,
    bins: int,
    total_count: int,
    unit: str,
) -> dict[str, Any]:
    cleaned = sorted(value for value in values if _finite(value))
    if not cleaned:
        return {
            "count": total_count,
            "sample_count": 0,
            "unit": unit,
            "min": None,
            "p25": None,
            "median": None,
            "p75": None,
            "p95": None,
            "max": None,
            "mean": None,
            "bins": [],
        }

    lower = cleaned[0]
    upper = cleaned[-1]
    histogram: list[dict[str, Any]] = []
    if lower == upper:
        histogram.append(
            {"lower": lower, "upper": upper, "count": len(cleaned)}
        )
    else:
        width = (upper - lower) / bins
        counts = [0] * bins
        for value in cleaned:
            index = min(int((value - lower) / width), bins - 1)
            counts[index] += 1
        for index, count in enumerate(counts):
            bin_lower = lower + width * index
            bin_upper = upper if index == bins - 1 else lower + width * (index + 1)
            histogram.append(
                {
                    "lower": bin_lower,
                    "upper": bin_upper,
                    "count": count,
                }
            )

    return {
        "count": total_count,
        "sample_count": len(cleaned),
        "unit": unit,
        "min": lower,
        "p25": _quantile(cleaned, 0.25),
        "median": _quantile(cleaned, 0.50),
        "p75": _quantile(cleaned, 0.75),
        "p95": _quantile(cleaned, 0.95),
        "max": upper,
        "mean": fmean(cleaned),
        "bins": histogram,
    }


def _quantile(values: list[float], quantile: float) -> float:
    if len(values) == 1:
        return values[0]
    position = (len(values) - 1) * quantile
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(values) - 1)
    fraction = position - lower_index
    return (
        values[lower_index] * (1.0 - fraction)
        + values[upper_index] * fraction
    )


def _decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return max(parsed, 0)


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _finite(value: float) -> bool:
    return value == value and value not in (float("inf"), float("-inf"))


def _float(value: Decimal | float | int) -> float:
    return float(value)
