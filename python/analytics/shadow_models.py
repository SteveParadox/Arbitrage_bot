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
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from analytics.db import Base

MONEY = Numeric(38, 18)
BPS = Numeric(20, 8)


class ShadowRun(Base):
    __tablename__ = "shadow_runs"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    base_asset: Mapped[str] = mapped_column(String(32), nullable=False)
    latency_ms: Mapped[list] = mapped_column(JSON, nullable=False)
    minimum_observations: Mapped[int] = mapped_column(Integer, nullable=False)
    observed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    sampled_observation_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    approved_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    would_execute_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    ready_for_analysis: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    no_order_endpoints: Mapped[bool] = mapped_column(Boolean, nullable=False)
    mainnet_market_data: Mapped[bool] = mapped_column(Boolean, nullable=False)
    mainnet_read_only_account: Mapped[bool] = mapped_column(Boolean, nullable=False)
    latest_account_snapshot: Mapped[dict | None] = mapped_column(JSON)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class ShadowObservation(Base):
    __tablename__ = "shadow_observations"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    run_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("shadow_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    route_id: Mapped[str] = mapped_column(String(255), nullable=False)
    triangle_id: Mapped[str] = mapped_column(String(255), nullable=False)
    start_asset: Mapped[str] = mapped_column(String(32), nullable=False)

    starting_capital: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    detection_final_amount: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    detection_gross_profit: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    expected_profit: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    latency_neutral_detection_profit: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    expected_net_edge_bps: Mapped[Decimal] = mapped_column(BPS, nullable=False)

    detected: Mapped[bool] = mapped_column(Boolean, nullable=False)
    approved: Mapped[bool] = mapped_column(Boolean, nullable=False)
    would_execute: Mapped[bool] = mapped_column(Boolean, nullable=False)
    approval_error: Mapped[str | None] = mapped_column(String(512))
    risk_checks: Mapped[list] = mapped_column(JSON, nullable=False)

    account_balance: Mapped[Decimal | None] = mapped_column(MONEY)
    account_equity_usd: Mapped[Decimal | None] = mapped_column(MONEY)
    account_exposure_usd: Mapped[Decimal | None] = mapped_column(MONEY)
    session_pnl_proxy_usd: Mapped[Decimal | None] = mapped_column(MONEY)

    detection_leg_prices: Mapped[list] = mapped_column(JSON, nullable=False)
    oldest_book_timestamp_ms: Mapped[int | None] = mapped_column(BigInteger)
    newest_book_timestamp_ms: Mapped[int | None] = mapped_column(BigInteger)
    book_timestamp_skew_ms: Mapped[int | None] = mapped_column(BigInteger)
    latency_tracking: Mapped[bool] = mapped_column(Boolean, nullable=False)
    raw_event: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        Index("ix_shadow_observations_run_detected", "run_id", "detected_at"),
        Index("ix_shadow_observations_route_detected", "route_id", "detected_at"),
        Index("ix_shadow_observations_approved", "run_id", "approved"),
        Index("ix_shadow_observations_would_execute", "run_id", "would_execute"),
    )


class ShadowLatencySample(Base):
    __tablename__ = "shadow_latency_samples"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("shadow_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    observation_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("shadow_observations.id", ondelete="CASCADE"),
        nullable=False,
    )
    route_id: Mapped[str] = mapped_column(String(255), nullable=False)
    latency_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    target_at_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    sampled_at_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    scheduler_lag_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    sample_valid: Mapped[bool] = mapped_column(Boolean, nullable=False)
    failure_reason: Mapped[str | None] = mapped_column(String(512))

    final_amount: Mapped[Decimal | None] = mapped_column(MONEY)
    net_profit: Mapped[Decimal | None] = mapped_column(MONEY)
    net_edge_bps: Mapped[Decimal | None] = mapped_column(BPS)
    profit_drift_from_detection: Mapped[Decimal | None] = mapped_column(MONEY)
    route_final_drift_bps: Mapped[Decimal | None] = mapped_column(BPS)
    leg_price_drift_bps: Mapped[list] = mapped_column(JSON, nullable=False)
    profitable_after_latency: Mapped[bool] = mapped_column(Boolean, nullable=False)
    still_meets_min_edge: Mapped[bool] = mapped_column(Boolean, nullable=False)

    leg_average_prices: Mapped[list] = mapped_column(JSON, nullable=False)
    oldest_book_timestamp_ms: Mapped[int | None] = mapped_column(BigInteger)
    newest_book_timestamp_ms: Mapped[int | None] = mapped_column(BigInteger)
    book_timestamp_skew_ms: Mapped[int | None] = mapped_column(BigInteger)
    raw_event: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint(
            "observation_id",
            "latency_ms",
            name="uq_shadow_observation_latency",
        ),
        Index("ix_shadow_samples_run_latency", "run_id", "latency_ms"),
        Index("ix_shadow_samples_route_latency", "route_id", "latency_ms"),
        Index(
            "ix_shadow_samples_survival",
            "run_id",
            "latency_ms",
            "profitable_after_latency",
        ),
    )
