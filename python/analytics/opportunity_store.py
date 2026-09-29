from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

from sqlalchemy import case, desc, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from analytics.models import OpportunityObservation, OpportunityWindow


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def _detected_at(scan: dict[str, Any]) -> datetime:
    milliseconds = int(scan["scan_timestamp"])
    return datetime.fromtimestamp(milliseconds / 1000, tz=UTC)


def _observation_key(scan: dict[str, Any]) -> str:
    identity = "|".join(
        [
            str(scan.get("route_id", "")),
            str(scan.get("scan_timestamp", "")),
            str(scan.get("trigger_symbol", "")),
            str(scan.get("trigger_update_id", "")),
            str(scan.get("trigger_sequence", "")),
        ]
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _liquidity(scan: dict[str, Any]) -> tuple[Decimal | None, Decimal | None]:
    status = scan.get("status")
    legs = scan.get("legs") or []
    start = _decimal(scan.get("start_amount"))

    if start is None:
        return None, None
    if status == "complete" and len(legs) == 3:
        return start, Decimal("1")
    if status != "insufficient_liquidity":
        return None, None

    ratios: list[Decimal] = []
    for leg in legs:
        execution = leg.get("execution") or {}
        side = str(leg.get("side", "")).upper()
        if side == "BUY":
            requested = _decimal(execution.get("requested_quote_quantity"))
            filled = _decimal(execution.get("filled_quote_quantity"))
        else:
            requested = _decimal(execution.get("requested_base_quantity"))
            filled = _decimal(execution.get("filled_base_quantity"))

        if requested is None or filled is None or requested <= 0:
            continue
        ratios.append(max(Decimal("0"), min(Decimal("1"), filled / requested)))

    if not ratios:
        return None, None

    ratio = min(ratios)
    return start * ratio, ratio


@dataclass(frozen=True)
class OpportunityDecision:
    executable: bool
    accepted: bool
    rejection_reason: str | None
    available_liquidity: Decimal | None
    available_liquidity_ratio: Decimal | None


def classify_scan(scan: dict[str, Any], min_net_edge_bps: Decimal) -> OpportunityDecision:
    status = str(scan.get("status", ""))
    legs = scan.get("legs") or []
    executable = (
        status == "complete"
        and len(legs) == 3
        and all(bool(leg.get("complete")) for leg in legs)
        and scan.get("start_amount") is not None
        and scan.get("final_amount") is not None
    )

    liquidity, liquidity_ratio = _liquidity(scan)

    if not executable:
        reason = {
            "missing_book": "missing_book",
            "insufficient_liquidity": "insufficient_liquidity",
            "start_amount_not_configured": "start_amount_not_configured",
            "calculation_error": "calculation_error",
        }.get(status, "incomplete_route")
        return OpportunityDecision(
            executable=False,
            accepted=False,
            rejection_reason=reason,
            available_liquidity=liquidity,
            available_liquidity_ratio=liquidity_ratio,
        )

    if not bool(scan.get("fees_included")):
        return OpportunityDecision(True, False, "fee_model_missing", liquidity, liquidity_ratio)

    net_profitable = scan.get("net_profitable")
    net_edge = _decimal(scan.get("expected_net_return_bps"))
    if net_profitable is not True or net_edge is None or net_edge <= 0:
        return OpportunityDecision(True, False, "net_not_profitable", liquidity, liquidity_ratio)

    if net_edge < min_net_edge_bps:
        return OpportunityDecision(True, False, "below_min_net_edge", liquidity, liquidity_ratio)

    return OpportunityDecision(True, True, None, liquidity, liquidity_ratio)


@dataclass
class ActiveWindow:
    id: str
    route_id: str
    triangle_id: str
    start_asset: str
    started_at: datetime
    last_seen_at: datetime
    duration_ms: int
    observation_count: int
    max_net_edge_bps: Decimal | None
    max_net_profit: Decimal | None


class OpportunityStore:
    def __init__(
        self,
        session: Session,
        *,
        min_net_edge_bps: Decimal = Decimal("0"),
        max_continuity_gap_ms: int = 2_000,
    ) -> None:
        if min_net_edge_bps < 0:
            raise ValueError("min_net_edge_bps must be non-negative")
        if max_continuity_gap_ms <= 0:
            raise ValueError("max_continuity_gap_ms must be greater than zero")

        self.session = session
        self.min_net_edge_bps = min_net_edge_bps
        self.max_continuity_gap_ms = max_continuity_gap_ms
        self._active_windows = self._load_active_windows()

    def _load_active_windows(self) -> dict[str, ActiveWindow]:
        rows = self.session.scalars(
            select(OpportunityWindow).where(OpportunityWindow.ended_at.is_(None))
        ).all()
        return {
            row.route_id: ActiveWindow(
                id=row.id,
                route_id=row.route_id,
                triangle_id=row.triangle_id,
                start_asset=row.start_asset,
                started_at=row.started_at,
                last_seen_at=row.last_seen_at,
                duration_ms=row.duration_ms,
                observation_count=row.observation_count,
                max_net_edge_bps=row.max_net_edge_bps,
                max_net_profit=row.max_net_profit,
            )
            for row in rows
        }

    def record_scan(self, scan: dict[str, Any]) -> bool:
        decision = classify_scan(scan, self.min_net_edge_bps)
        detected_at = _detected_at(scan)
        route_id = str(scan["route_id"])
        triangle_id = str(scan["triangle_id"])
        start_asset = str(scan["start_asset"])
        net_edge = _decimal(scan.get("expected_net_return_bps"))
        net_profit = _decimal(scan.get("expected_net_profit"))

        window_id: str | None = None
        duration_ms = 0
        active = self._active_windows.get(route_id)

        if decision.accepted:
            if active is not None:
                gap_ms = int((detected_at - active.last_seen_at).total_seconds() * 1000)
                if gap_ms > self.max_continuity_gap_ms:
                    self._close_window(active, active.last_seen_at, "continuity_gap")
                    active = None

            if active is None:
                active = ActiveWindow(
                    id=str(uuid4()),
                    route_id=route_id,
                    triangle_id=triangle_id,
                    start_asset=start_asset,
                    started_at=detected_at,
                    last_seen_at=detected_at,
                    duration_ms=0,
                    observation_count=0,
                    max_net_edge_bps=net_edge,
                    max_net_profit=net_profit,
                )

            duration_ms = max(
                0, int((detected_at - active.started_at).total_seconds() * 1000)
            )
            window_id = active.id

        values = self._observation_values(
            scan,
            decision,
            detected_at,
            window_id,
            duration_ms,
        )
        statement = (
            insert(OpportunityObservation)
            .values(**values)
            .on_conflict_do_nothing(index_elements=["observation_key"])
            .returning(OpportunityObservation.id)
        )
        inserted_id = self.session.execute(statement).scalar_one_or_none()
        if inserted_id is None:
            return False

        if decision.accepted and active is not None:
            active.last_seen_at = detected_at
            active.duration_ms = duration_ms
            active.observation_count += 1
            active.max_net_edge_bps = _max_decimal(active.max_net_edge_bps, net_edge)
            active.max_net_profit = _max_decimal(active.max_net_profit, net_profit)
            self._upsert_window(active)
            self._active_windows[route_id] = active
        elif active is not None:
            self._close_window(active, detected_at, decision.rejection_reason or "rejected")
            self._active_windows.pop(route_id, None)

        return True

    def _observation_values(
        self,
        scan: dict[str, Any],
        decision: OpportunityDecision,
        detected_at: datetime,
        window_id: str | None,
        duration_ms: int,
    ) -> dict[str, Any]:
        profitability = scan.get("profitability") or {}
        return {
            "observation_key": _observation_key(scan),
            "detected_at": detected_at,
            "scan_timestamp_ms": int(scan["scan_timestamp"]),
            "trigger_symbol": str(scan["trigger_symbol"]),
            "trigger_update_id": int(scan["trigger_update_id"]),
            "trigger_sequence": int(scan["trigger_sequence"]),
            "route_id": str(scan["route_id"]),
            "triangle_id": str(scan["triangle_id"]),
            "start_asset": str(scan["start_asset"]),
            "opportunity_window_id": window_id,
            "starting_capital": _decimal(scan.get("start_amount")),
            "gross_final_amount": _decimal(scan.get("final_amount")),
            "gross_profit": _decimal(scan.get("gross_profit")),
            "gross_edge_bps": _decimal(scan.get("gross_return_bps")),
            "gross_edge_pct": _decimal(scan.get("gross_return_pct")),
            "fee_cost": _decimal(profitability.get("fee_amount")),
            "fee_cost_bps": _decimal(profitability.get("fee_bps_on_start")),
            "estimated_slippage": _decimal(profitability.get("expected_slippage_amount")),
            "estimated_slippage_bps": _decimal(
                profitability.get("expected_slippage_bps")
            ),
            "rounding_loss": _decimal(profitability.get("rounding_loss_amount")),
            "rounding_loss_bps": _decimal(profitability.get("rounding_loss_bps")),
            "latency_buffer": _decimal(profitability.get("latency_buffer_amount")),
            "latency_buffer_bps": _decimal(profitability.get("latency_buffer_bps")),
            "safety_margin": _decimal(profitability.get("safety_margin_amount")),
            "safety_margin_bps": _decimal(profitability.get("safety_margin_bps")),
            "total_cost": _decimal(profitability.get("total_cost_amount")),
            "total_cost_bps": _decimal(profitability.get("total_cost_bps")),
            "expected_final_amount": _decimal(scan.get("expected_final_amount")),
            "net_profit": _decimal(scan.get("expected_net_profit")),
            "net_edge_bps": _decimal(scan.get("expected_net_return_bps")),
            "net_edge_pct": _decimal(scan.get("expected_net_return_pct")),
            "available_liquidity": decision.available_liquidity,
            "available_liquidity_ratio": decision.available_liquidity_ratio,
            "opportunity_duration_ms": duration_ms,
            "scanner_status": str(scan.get("status", "")),
            "executable": decision.executable,
            "accepted": decision.accepted,
            "rejection_reason": decision.rejection_reason,
            "gross_profitable": scan.get("gross_profitable"),
            "net_profitable": scan.get("net_profitable"),
            "fees_included": bool(scan.get("fees_included")),
            "book_timestamp_skew_ms": scan.get("book_timestamp_skew_ms"),
            "raw_scan": scan,
        }

    def _upsert_window(self, active: ActiveWindow) -> None:
        statement = insert(OpportunityWindow).values(
            id=active.id,
            route_id=active.route_id,
            triangle_id=active.triangle_id,
            start_asset=active.start_asset,
            started_at=active.started_at,
            last_seen_at=active.last_seen_at,
            ended_at=None,
            duration_ms=active.duration_ms,
            observation_count=active.observation_count,
            max_net_edge_bps=active.max_net_edge_bps,
            max_net_profit=active.max_net_profit,
            close_reason=None,
        )
        statement = statement.on_conflict_do_update(
            index_elements=["id"],
            set_={
                "last_seen_at": active.last_seen_at,
                "duration_ms": active.duration_ms,
                "observation_count": active.observation_count,
                "max_net_edge_bps": active.max_net_edge_bps,
                "max_net_profit": active.max_net_profit,
                "ended_at": None,
                "close_reason": None,
            },
        )
        self.session.execute(statement)

    def _close_window(self, active: ActiveWindow, ended_at: datetime, reason: str) -> None:
        duration_ms = max(
            active.duration_ms,
            int((ended_at - active.started_at).total_seconds() * 1000),
        )
        self.session.execute(
            update(OpportunityWindow)
            .where(OpportunityWindow.id == active.id)
            .values(
                last_seen_at=max(active.last_seen_at, ended_at),
                ended_at=ended_at,
                duration_ms=duration_ms,
                close_reason=reason,
            )
        )


def _max_decimal(first: Decimal | None, second: Decimal | None) -> Decimal | None:
    if first is None:
        return second
    if second is None:
        return first
    return max(first, second)


def analytics_summary(session: Session, hours: int = 24) -> dict[str, Any]:
    since = datetime.now(UTC) - timedelta(hours=hours)
    base = OpportunityObservation.detected_at >= since

    totals = session.execute(
        select(
            func.count().label("detected"),
            func.count().filter(OpportunityObservation.executable.is_(True)).label("executable"),
            func.count().filter(OpportunityObservation.accepted.is_(True)).label("accepted"),
            func.count().filter(OpportunityObservation.accepted.is_(False)).label("rejected"),
            func.count()
            .filter(OpportunityObservation.gross_profitable.is_(True))
            .label("gross_profitable"),
            func.count()
            .filter(OpportunityObservation.net_profitable.is_(True))
            .label("net_profitable"),
            func.avg(OpportunityObservation.net_edge_bps)
            .filter(OpportunityObservation.accepted.is_(True))
            .label("avg_accepted_net_edge_bps"),
            func.max(OpportunityObservation.net_edge_bps).label("max_net_edge_bps"),
        ).where(base)
    ).one()

    window_stats = session.execute(
        select(
            func.count(OpportunityWindow.id),
            func.avg(OpportunityWindow.duration_ms),
            func.max(OpportunityWindow.duration_ms),
        ).where(OpportunityWindow.started_at >= since)
    ).one()

    detected = int(totals.detected or 0)
    executable = int(totals.executable or 0)
    accepted = int(totals.accepted or 0)

    return {
        "hours": hours,
        "detected": detected,
        "executable": executable,
        "accepted": accepted,
        "rejected": int(totals.rejected or 0),
        "gross_profitable": int(totals.gross_profitable or 0),
        "net_profitable": int(totals.net_profitable or 0),
        "executable_rate_pct": _percentage(executable, detected),
        "accepted_rate_pct": _percentage(accepted, detected),
        "accepted_of_executable_pct": _percentage(accepted, executable),
        "avg_accepted_net_edge_bps": _float_or_none(totals.avg_accepted_net_edge_bps),
        "max_net_edge_bps": _float_or_none(totals.max_net_edge_bps),
        "opportunity_windows": int(window_stats[0] or 0),
        "avg_window_duration_ms": _float_or_none(window_stats[1]),
        "max_window_duration_ms": int(window_stats[2] or 0),
    }


def rejection_breakdown(session: Session, hours: int = 24) -> list[dict[str, Any]]:
    since = datetime.now(UTC) - timedelta(hours=hours)
    rows = session.execute(
        select(
            OpportunityObservation.rejection_reason,
            func.count().label("count"),
        )
        .where(
            OpportunityObservation.detected_at >= since,
            OpportunityObservation.accepted.is_(False),
        )
        .group_by(OpportunityObservation.rejection_reason)
        .order_by(desc("count"))
    ).all()
    return [
        {"reason": reason or "unknown", "count": int(count)}
        for reason, count in rows
    ]


def triangle_breakdown(
    session: Session,
    hours: int = 24,
    limit: int = 20,
) -> list[dict[str, Any]]:
    since = datetime.now(UTC) - timedelta(hours=hours)
    rows = session.execute(
        select(
            OpportunityObservation.triangle_id,
            func.count().label("detected"),
            func.sum(case((OpportunityObservation.executable.is_(True), 1), else_=0)).label(
                "executable"
            ),
            func.sum(case((OpportunityObservation.accepted.is_(True), 1), else_=0)).label(
                "accepted"
            ),
            func.avg(OpportunityObservation.net_edge_bps)
            .filter(OpportunityObservation.net_edge_bps.is_not(None))
            .label("avg_net_edge_bps"),
            func.max(OpportunityObservation.net_edge_bps).label("max_net_edge_bps"),
        )
        .where(OpportunityObservation.detected_at >= since)
        .group_by(OpportunityObservation.triangle_id)
        .order_by(desc("accepted"), desc("detected"))
        .limit(limit)
    ).all()

    return [
        {
            "triangle": row.triangle_id,
            "detected": int(row.detected or 0),
            "executable": int(row.executable or 0),
            "accepted": int(row.accepted or 0),
            "avg_net_edge_bps": _float_or_none(row.avg_net_edge_bps),
            "max_net_edge_bps": _float_or_none(row.max_net_edge_bps),
        }
        for row in rows
    ]


def _percentage(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 4)


def _float_or_none(value: Any) -> float | None:
    return None if value is None else float(value)
