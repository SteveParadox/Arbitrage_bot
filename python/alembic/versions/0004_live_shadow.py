"""Add live shadow-mode observations and latency samples.

Revision ID: 0004_live_shadow
Revises: 0003_paper_remaining_lifetime
Create Date: 2026-09-30
"""

from alembic import op
import sqlalchemy as sa

revision = "0004_live_shadow"
down_revision = "0003_paper_remaining_lifetime"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "shadow_runs",
        sa.Column("id", sa.String(length=64), primary_key=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("base_asset", sa.String(length=32), nullable=False),
        sa.Column("latency_ms", sa.JSON(), nullable=False),
        sa.Column("minimum_observations", sa.Integer(), nullable=False),
        sa.Column("observed_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "sampled_observation_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column("approved_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "would_execute_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "ready_for_analysis",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column("no_order_endpoints", sa.Boolean(), nullable=False),
        sa.Column("mainnet_market_data", sa.Boolean(), nullable=False),
        sa.Column("mainnet_read_only_account", sa.Boolean(), nullable=False),
        sa.Column("latest_account_snapshot", sa.JSON()),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )

    op.create_table(
        "shadow_observations",
        sa.Column("id", sa.String(length=64), primary_key=True),
        sa.Column(
            "run_id",
            sa.String(length=64),
            sa.ForeignKey("shadow_runs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("route_id", sa.String(length=255), nullable=False),
        sa.Column("triangle_id", sa.String(length=255), nullable=False),
        sa.Column("start_asset", sa.String(length=32), nullable=False),
        sa.Column("starting_capital", sa.Numeric(38, 18), nullable=False),
        sa.Column("detection_final_amount", sa.Numeric(38, 18), nullable=False),
        sa.Column("detection_gross_profit", sa.Numeric(38, 18), nullable=False),
        sa.Column("expected_profit", sa.Numeric(38, 18), nullable=False),
        sa.Column(
            "latency_neutral_detection_profit",
            sa.Numeric(38, 18),
            nullable=False,
        ),
        sa.Column("expected_net_edge_bps", sa.Numeric(20, 8), nullable=False),
        sa.Column("detected", sa.Boolean(), nullable=False),
        sa.Column("approved", sa.Boolean(), nullable=False),
        sa.Column("would_execute", sa.Boolean(), nullable=False),
        sa.Column("approval_error", sa.String(length=512)),
        sa.Column("risk_checks", sa.JSON(), nullable=False),
        sa.Column("account_balance", sa.Numeric(38, 18)),
        sa.Column("account_equity_usd", sa.Numeric(38, 18)),
        sa.Column("account_exposure_usd", sa.Numeric(38, 18)),
        sa.Column("session_pnl_proxy_usd", sa.Numeric(38, 18)),
        sa.Column("detection_leg_prices", sa.JSON(), nullable=False),
        sa.Column("oldest_book_timestamp_ms", sa.BigInteger()),
        sa.Column("newest_book_timestamp_ms", sa.BigInteger()),
        sa.Column("book_timestamp_skew_ms", sa.BigInteger()),
        sa.Column("latency_tracking", sa.Boolean(), nullable=False),
        sa.Column("raw_event", sa.JSON(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ix_shadow_observations_run_detected",
        "shadow_observations",
        ["run_id", "detected_at"],
    )
    op.create_index(
        "ix_shadow_observations_route_detected",
        "shadow_observations",
        ["route_id", "detected_at"],
    )
    op.create_index(
        "ix_shadow_observations_approved",
        "shadow_observations",
        ["run_id", "approved"],
    )
    op.create_index(
        "ix_shadow_observations_would_execute",
        "shadow_observations",
        ["run_id", "would_execute"],
    )

    op.create_table(
        "shadow_latency_samples",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "run_id",
            sa.String(length=64),
            sa.ForeignKey("shadow_runs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "observation_id",
            sa.String(length=64),
            sa.ForeignKey("shadow_observations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("route_id", sa.String(length=255), nullable=False),
        sa.Column("latency_ms", sa.Integer(), nullable=False),
        sa.Column("target_at_ms", sa.BigInteger(), nullable=False),
        sa.Column("sampled_at_ms", sa.BigInteger(), nullable=False),
        sa.Column("scheduler_lag_ms", sa.BigInteger(), nullable=False),
        sa.Column("sample_valid", sa.Boolean(), nullable=False),
        sa.Column("failure_reason", sa.String(length=512)),
        sa.Column("final_amount", sa.Numeric(38, 18)),
        sa.Column("net_profit", sa.Numeric(38, 18)),
        sa.Column("net_edge_bps", sa.Numeric(20, 8)),
        sa.Column("profit_drift_from_detection", sa.Numeric(38, 18)),
        sa.Column("route_final_drift_bps", sa.Numeric(20, 8)),
        sa.Column("leg_price_drift_bps", sa.JSON(), nullable=False),
        sa.Column("profitable_after_latency", sa.Boolean(), nullable=False),
        sa.Column("still_meets_min_edge", sa.Boolean(), nullable=False),
        sa.Column("leg_average_prices", sa.JSON(), nullable=False),
        sa.Column("oldest_book_timestamp_ms", sa.BigInteger()),
        sa.Column("newest_book_timestamp_ms", sa.BigInteger()),
        sa.Column("book_timestamp_skew_ms", sa.BigInteger()),
        sa.Column("raw_event", sa.JSON(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "observation_id",
            "latency_ms",
            name="uq_shadow_observation_latency",
        ),
    )
    op.create_index(
        "ix_shadow_samples_run_latency",
        "shadow_latency_samples",
        ["run_id", "latency_ms"],
    )
    op.create_index(
        "ix_shadow_samples_route_latency",
        "shadow_latency_samples",
        ["route_id", "latency_ms"],
    )
    op.create_index(
        "ix_shadow_samples_survival",
        "shadow_latency_samples",
        ["run_id", "latency_ms", "profitable_after_latency"],
    )


def downgrade() -> None:
    op.drop_table("shadow_latency_samples")
    op.drop_table("shadow_observations")
    op.drop_table("shadow_runs")
