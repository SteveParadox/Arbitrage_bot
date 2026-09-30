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
    false,
)
from sqlalchemy.orm import Mapped, mapped_column

from analytics.db import Base

MONEY = Numeric(38, 18)
BPS = Numeric(20, 8)


class MarketBookEvent(Base):
    __tablename__ = "market_book_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    event_timestamp_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    update_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    is_snapshot: Mapped[bool] = mapped_column(Boolean, nullable=False)
    bids: Mapped[list] = mapped_column(JSON, nullable=False)
    asks: Mapped[list] = mapped_column(JSON, nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint(
            "symbol",
            "event_timestamp_ms",
            "update_id",
            "sequence",
            name="uq_market_book_event_identity",
        ),
        Index(
            "ix_market_book_events_symbol_timestamp",
            "symbol",
            "event_timestamp_ms",
        ),
        Index(
            "ix_market_book_events_snapshot_lookup",
            "symbol",
            "is_snapshot",
            "event_timestamp_ms",
        ),
    )


class PaperSimulationRun(Base):
    __tablename__ = "paper_simulation_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source_from_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    source_to_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    requested_limit: Mapped[int] = mapped_column(Integer, nullable=False)
    opportunity_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    scenario_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    config: Mapped[dict] = mapped_column(JSON, nullable=False)
    summary: Mapped[dict | None] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="running", server_default="running"
    )
    failure_reason: Mapped[str | None] = mapped_column(String(255))


class PaperSimulationResult(Base):
    __tablename__ = "paper_simulation_results"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("paper_simulation_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    opportunity_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("opportunity_observations.id", ondelete="CASCADE"),
        nullable=False,
    )
    route_id: Mapped[str] = mapped_column(String(255), nullable=False)
    triangle_id: Mapped[str] = mapped_column(String(255), nullable=False)
    detected_at_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    latency_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    total_execution_time_ms: Mapped[int] = mapped_column(Integer, nullable=False)

    starting_capital: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    expected_profit: Mapped[Decimal | None] = mapped_column(MONEY)
    expected_net_edge_bps: Mapped[Decimal | None] = mapped_column(BPS)
    detected_gross_final_amount: Mapped[Decimal | None] = mapped_column(MONEY)

    simulated_final_amount: Mapped[Decimal | None] = mapped_column(MONEY)
    simulated_profit: Mapped[Decimal | None] = mapped_column(MONEY)
    simulated_net_edge_bps: Mapped[Decimal | None] = mapped_column(BPS)
    execution_drift_amount: Mapped[Decimal | None] = mapped_column(MONEY)
    execution_drift_bps: Mapped[Decimal | None] = mapped_column(BPS)
    expectation_error: Mapped[Decimal | None] = mapped_column(MONEY)

    completed: Mapped[bool] = mapped_column(Boolean, nullable=False)
    fill_ratio: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    failure_reason: Mapped[str | None] = mapped_column(String(128))
    failure_leg: Mapped[int | None] = mapped_column(Integer)
    opportunity_lifetime_ms: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0, server_default="0"
    )
    remaining_lifetime_ms: Mapped[int | None] = mapped_column(BigInteger)
    outlived_opportunity: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=false()
    )
    max_book_age_ms: Mapped[int | None] = mapped_column(BigInteger)
    legs: Mapped[list] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint(
            "run_id",
            "opportunity_id",
            "latency_ms",
            name="uq_paper_simulation_run_opportunity_latency",
        ),
        Index("ix_paper_simulation_results_run_latency", "run_id", "latency_ms"),
        Index("ix_paper_simulation_results_route_latency", "route_id", "latency_ms"),
        Index("ix_paper_simulation_results_completed", "run_id", "completed"),
        Index("ix_paper_simulation_results_failure", "run_id", "failure_reason"),
    )
