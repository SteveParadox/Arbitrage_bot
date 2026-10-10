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
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from analytics.db import Base

MONEY = Numeric(38, 18)
BPS = Numeric(20, 8)


class MicroLiveRun(Base):
    __tablename__ = "micro_live_runs"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    base_asset: Mapped[str] = mapped_column(String(32), nullable=False)
    cycle_notional: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    hard_cycle_cap: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    manual_execution_required: Mapped[bool] = mapped_column(
        Boolean, nullable=False
    )
    candidates_recorded: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    reconciled_cycles: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class MicroLiveCycle(Base):
    __tablename__ = "micro_live_cycles"

    trade_id: Mapped[str] = mapped_column(String(96), primary_key=True)
    session_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("micro_live_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    route_id: Mapped[str] = mapped_column(String(255), nullable=False)
    triangle_id: Mapped[str] = mapped_column(String(255), nullable=False)
    base_asset: Mapped[str] = mapped_column(String(32), nullable=False)

    starting_capital: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    expected_pnl: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    expected_fees: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    expected_slippage: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    expected_slippage_bps: Mapped[Decimal] = mapped_column(BPS, nullable=False)
    expected_net_edge_bps: Mapped[Decimal] = mapped_column(BPS, nullable=False)
    fee_bps_per_leg: Mapped[list] = mapped_column(JSON, nullable=False)
    detection_leg_prices: Mapped[list] = mapped_column(JSON, nullable=False)

    account_balance: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    account_equity_usd: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    account_exposure_usd: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    manual_execution_required: Mapped[bool] = mapped_column(
        Boolean, nullable=False
    )

    realized_pnl: Mapped[Decimal | None] = mapped_column(MONEY)
    prediction_error: Mapped[Decimal | None] = mapped_column(MONEY)
    actual_fee_amount_base: Mapped[Decimal | None] = mapped_column(MONEY)
    actual_fees_by_currency: Mapped[dict | None] = mapped_column(JSON)
    actual_slippage: Mapped[Decimal | None] = mapped_column(MONEY)
    actual_slippage_bps: Mapped[Decimal | None] = mapped_column(BPS)
    execution_time_ms: Mapped[int | None] = mapped_column(BigInteger)
    execution_status: Mapped[str | None] = mapped_column(String(64))
    notes: Mapped[str | None] = mapped_column(String(1024))
    reconciled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    reconciliation_digest: Mapped[str | None] = mapped_column(String(64))
    reconciliation_request_id: Mapped[str | None] = mapped_column(String(64))
    raw_candidate_event: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("reconciliation_request_id", name="uq_micro_live_reconciliation_request"),
        Index("ix_micro_live_cycle_session_detected", "session_id", "detected_at"),
        Index("ix_micro_live_cycle_route_detected", "route_id", "detected_at"),
        Index(
            "ix_micro_live_cycle_prediction_error",
            "session_id",
            "prediction_error",
        ),
    )
