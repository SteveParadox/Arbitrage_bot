from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    Numeric,
    String,
    func,
    false,
)
from sqlalchemy.orm import Mapped, mapped_column

from analytics.db import Base

MONEY = Numeric(38, 18)
BPS = Numeric(20, 8)
RATIO = Numeric(20, 10)


class OpportunityWindow(Base):
    __tablename__ = "opportunity_windows"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    route_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    triangle_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    start_asset: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    duration_ms: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    observation_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    max_net_edge_bps: Mapped[Decimal | None] = mapped_column(BPS)
    max_net_profit: Mapped[Decimal | None] = mapped_column(MONEY)
    close_reason: Mapped[str | None] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        Index("ix_opportunity_windows_started_at", "started_at"),
        Index("ix_opportunity_windows_route_started", "route_id", "started_at"),
        Index("ix_opportunity_windows_open", "route_id", "ended_at"),
    )


class OpportunityObservation(Base):
    __tablename__ = "opportunity_observations"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    observation_key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    scan_timestamp_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    trigger_symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    trigger_update_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    trigger_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)

    route_id: Mapped[str] = mapped_column(String(255), nullable=False)
    triangle_id: Mapped[str] = mapped_column(String(255), nullable=False)
    start_asset: Mapped[str] = mapped_column(String(32), nullable=False)
    opportunity_window_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey(
            "opportunity_windows.id",
            ondelete="SET NULL",
            deferrable=True,
            initially="DEFERRED",
        )
    )

    starting_capital: Mapped[Decimal | None] = mapped_column(MONEY)
    gross_final_amount: Mapped[Decimal | None] = mapped_column(MONEY)
    gross_profit: Mapped[Decimal | None] = mapped_column(MONEY)
    gross_edge_bps: Mapped[Decimal | None] = mapped_column(BPS)
    gross_edge_pct: Mapped[Decimal | None] = mapped_column(BPS)

    fee_cost: Mapped[Decimal | None] = mapped_column(MONEY)
    fee_cost_bps: Mapped[Decimal | None] = mapped_column(BPS)
    estimated_slippage: Mapped[Decimal | None] = mapped_column(MONEY)
    estimated_slippage_bps: Mapped[Decimal | None] = mapped_column(BPS)
    rounding_loss: Mapped[Decimal | None] = mapped_column(MONEY)
    rounding_loss_bps: Mapped[Decimal | None] = mapped_column(BPS)
    latency_buffer: Mapped[Decimal | None] = mapped_column(MONEY)
    latency_buffer_bps: Mapped[Decimal | None] = mapped_column(BPS)
    safety_margin: Mapped[Decimal | None] = mapped_column(MONEY)
    safety_margin_bps: Mapped[Decimal | None] = mapped_column(BPS)
    total_cost: Mapped[Decimal | None] = mapped_column(MONEY)
    total_cost_bps: Mapped[Decimal | None] = mapped_column(BPS)

    expected_final_amount: Mapped[Decimal | None] = mapped_column(MONEY)
    net_profit: Mapped[Decimal | None] = mapped_column(MONEY)
    net_edge_bps: Mapped[Decimal | None] = mapped_column(BPS)
    net_edge_pct: Mapped[Decimal | None] = mapped_column(BPS)

    available_liquidity: Mapped[Decimal | None] = mapped_column(MONEY)
    available_liquidity_ratio: Mapped[Decimal | None] = mapped_column(RATIO)
    opportunity_duration_ms: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0, server_default="0"
    )

    scanner_status: Mapped[str] = mapped_column(String(64), nullable=False)
    executable: Mapped[bool] = mapped_column(Boolean, nullable=False)
    accepted: Mapped[bool] = mapped_column(Boolean, nullable=False)
    rejection_reason: Mapped[str | None] = mapped_column(String(128))
    gross_profitable: Mapped[bool | None] = mapped_column(Boolean)
    net_profitable: Mapped[bool | None] = mapped_column(Boolean)
    fees_included: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=false()
    )
    book_timestamp_skew_ms: Mapped[int | None] = mapped_column(BigInteger)

    raw_scan: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        Index("ix_opportunity_observations_detected_at", "detected_at"),
        Index("ix_opportunity_observations_triangle_detected", "triangle_id", "detected_at"),
        Index("ix_opportunity_observations_route_detected", "route_id", "detected_at"),
        Index("ix_opportunity_observations_executable_detected", "executable", "detected_at"),
        Index("ix_opportunity_observations_accepted_detected", "accepted", "detected_at"),
        Index("ix_opportunity_observations_rejection_detected", "rejection_reason", "detected_at"),
    )
