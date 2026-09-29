"""Add market-book archive and paper-trading simulation tables.

Revision ID: 0002_paper_simulation
Revises: 0001_opportunity_ledger
Create Date: 2026-09-29
"""

from alembic import op
import sqlalchemy as sa

revision = "0002_paper_simulation"
down_revision = "0001_opportunity_ledger"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "market_book_events",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("symbol", sa.String(length=64), nullable=False),
        sa.Column("event_timestamp_ms", sa.BigInteger(), nullable=False),
        sa.Column("update_id", sa.BigInteger(), nullable=False),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("is_snapshot", sa.Boolean(), nullable=False),
        sa.Column("bids", sa.JSON(), nullable=False),
        sa.Column("asks", sa.JSON(), nullable=False),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "symbol",
            "event_timestamp_ms",
            "update_id",
            "sequence",
            name="uq_market_book_event_identity",
        ),
    )
    op.create_index(
        "ix_market_book_events_symbol_timestamp",
        "market_book_events",
        ["symbol", "event_timestamp_ms"],
    )
    op.create_index(
        "ix_market_book_events_snapshot_lookup",
        "market_book_events",
        ["symbol", "is_snapshot", "event_timestamp_ms"],
    )

    op.create_table(
        "paper_simulation_runs",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("source_from_ms", sa.BigInteger(), nullable=False),
        sa.Column("source_to_ms", sa.BigInteger(), nullable=False),
        sa.Column("requested_limit", sa.Integer(), nullable=False),
        sa.Column("opportunity_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("scenario_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("config", sa.JSON(), nullable=False),
        sa.Column("summary", sa.JSON()),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="running"),
        sa.Column("failure_reason", sa.String(length=255)),
    )

    op.create_table(
        "paper_simulation_results",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "run_id",
            sa.String(length=36),
            sa.ForeignKey("paper_simulation_runs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "opportunity_id",
            sa.BigInteger(),
            sa.ForeignKey("opportunity_observations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("route_id", sa.String(length=255), nullable=False),
        sa.Column("triangle_id", sa.String(length=255), nullable=False),
        sa.Column("detected_at_ms", sa.BigInteger(), nullable=False),
        sa.Column("latency_ms", sa.Integer(), nullable=False),
        sa.Column("total_execution_time_ms", sa.Integer(), nullable=False),
        sa.Column("starting_capital", sa.Numeric(38, 18), nullable=False),
        sa.Column("expected_profit", sa.Numeric(38, 18)),
        sa.Column("expected_net_edge_bps", sa.Numeric(20, 8)),
        sa.Column("detected_gross_final_amount", sa.Numeric(38, 18)),
        sa.Column("simulated_final_amount", sa.Numeric(38, 18)),
        sa.Column("simulated_profit", sa.Numeric(38, 18)),
        sa.Column("simulated_net_edge_bps", sa.Numeric(20, 8)),
        sa.Column("execution_drift_amount", sa.Numeric(38, 18)),
        sa.Column("execution_drift_bps", sa.Numeric(20, 8)),
        sa.Column("expectation_error", sa.Numeric(38, 18)),
        sa.Column("completed", sa.Boolean(), nullable=False),
        sa.Column("fill_ratio", sa.Numeric(20, 10), nullable=False),
        sa.Column("failure_reason", sa.String(length=128)),
        sa.Column("failure_leg", sa.Integer()),
        sa.Column(
            "opportunity_lifetime_ms",
            sa.BigInteger(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "outlived_opportunity",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column("max_book_age_ms", sa.BigInteger()),
        sa.Column("legs", sa.JSON(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "run_id",
            "opportunity_id",
            "latency_ms",
            name="uq_paper_simulation_run_opportunity_latency",
        ),
    )
    op.create_index(
        "ix_paper_simulation_results_run_latency",
        "paper_simulation_results",
        ["run_id", "latency_ms"],
    )
    op.create_index(
        "ix_paper_simulation_results_route_latency",
        "paper_simulation_results",
        ["route_id", "latency_ms"],
    )
    op.create_index(
        "ix_paper_simulation_results_completed",
        "paper_simulation_results",
        ["run_id", "completed"],
    )
    op.create_index(
        "ix_paper_simulation_results_failure",
        "paper_simulation_results",
        ["run_id", "failure_reason"],
    )


def downgrade() -> None:
    op.drop_table("paper_simulation_results")
    op.drop_table("paper_simulation_runs")
    op.drop_table("market_book_events")
