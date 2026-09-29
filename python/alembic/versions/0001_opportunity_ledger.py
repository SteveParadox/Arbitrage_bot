"""Create opportunity ledger and continuous opportunity windows.

Revision ID: 0001_opportunity_ledger
Revises:
Create Date: 2026-09-29
"""

from alembic import op
import sqlalchemy as sa

revision = "0001_opportunity_ledger"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "opportunity_windows",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("route_id", sa.String(length=255), nullable=False),
        sa.Column("triangle_id", sa.String(length=255), nullable=False),
        sa.Column("start_asset", sa.String(length=32), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True)),
        sa.Column("duration_ms", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("observation_count", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("max_net_edge_bps", sa.Numeric(20, 8)),
        sa.Column("max_net_profit", sa.Numeric(38, 18)),
        sa.Column("close_reason", sa.String(length=128)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index("ix_opportunity_windows_started_at", "opportunity_windows", ["started_at"])
    op.create_index(
        "ix_opportunity_windows_route_started",
        "opportunity_windows",
        ["route_id", "started_at"],
    )
    op.create_index(
        "ix_opportunity_windows_open",
        "opportunity_windows",
        ["route_id", "ended_at"],
    )
    op.create_index("ix_opportunity_windows_route_id", "opportunity_windows", ["route_id"])
    op.create_index(
        "ix_opportunity_windows_triangle_id", "opportunity_windows", ["triangle_id"]
    )

    op.create_table(
        "opportunity_observations",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("observation_key", sa.String(length=64), nullable=False, unique=True),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("scan_timestamp_ms", sa.BigInteger(), nullable=False),
        sa.Column("trigger_symbol", sa.String(length=64), nullable=False),
        sa.Column("trigger_update_id", sa.BigInteger(), nullable=False),
        sa.Column("trigger_sequence", sa.BigInteger(), nullable=False),
        sa.Column("route_id", sa.String(length=255), nullable=False),
        sa.Column("triangle_id", sa.String(length=255), nullable=False),
        sa.Column("start_asset", sa.String(length=32), nullable=False),
        sa.Column(
            "opportunity_window_id",
            sa.String(length=36),
            sa.ForeignKey(
                "opportunity_windows.id",
                ondelete="SET NULL",
                deferrable=True,
                initially="DEFERRED",
            ),
        ),
        sa.Column("starting_capital", sa.Numeric(38, 18)),
        sa.Column("gross_final_amount", sa.Numeric(38, 18)),
        sa.Column("gross_profit", sa.Numeric(38, 18)),
        sa.Column("gross_edge_bps", sa.Numeric(20, 8)),
        sa.Column("gross_edge_pct", sa.Numeric(20, 8)),
        sa.Column("fee_cost", sa.Numeric(38, 18)),
        sa.Column("fee_cost_bps", sa.Numeric(20, 8)),
        sa.Column("estimated_slippage", sa.Numeric(38, 18)),
        sa.Column("estimated_slippage_bps", sa.Numeric(20, 8)),
        sa.Column("rounding_loss", sa.Numeric(38, 18)),
        sa.Column("rounding_loss_bps", sa.Numeric(20, 8)),
        sa.Column("latency_buffer", sa.Numeric(38, 18)),
        sa.Column("latency_buffer_bps", sa.Numeric(20, 8)),
        sa.Column("safety_margin", sa.Numeric(38, 18)),
        sa.Column("safety_margin_bps", sa.Numeric(20, 8)),
        sa.Column("total_cost", sa.Numeric(38, 18)),
        sa.Column("total_cost_bps", sa.Numeric(20, 8)),
        sa.Column("expected_final_amount", sa.Numeric(38, 18)),
        sa.Column("net_profit", sa.Numeric(38, 18)),
        sa.Column("net_edge_bps", sa.Numeric(20, 8)),
        sa.Column("net_edge_pct", sa.Numeric(20, 8)),
        sa.Column("available_liquidity", sa.Numeric(38, 18)),
        sa.Column("available_liquidity_ratio", sa.Numeric(20, 10)),
        sa.Column("opportunity_duration_ms", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("scanner_status", sa.String(length=64), nullable=False),
        sa.Column("executable", sa.Boolean(), nullable=False),
        sa.Column("accepted", sa.Boolean(), nullable=False),
        sa.Column("rejection_reason", sa.String(length=128)),
        sa.Column("gross_profitable", sa.Boolean()),
        sa.Column("net_profitable", sa.Boolean()),
        sa.Column("fees_included", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("book_timestamp_skew_ms", sa.BigInteger()),
        sa.Column("raw_scan", sa.JSON(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ix_opportunity_observations_detected_at",
        "opportunity_observations",
        ["detected_at"],
    )
    op.create_index(
        "ix_opportunity_observations_triangle_detected",
        "opportunity_observations",
        ["triangle_id", "detected_at"],
    )
    op.create_index(
        "ix_opportunity_observations_route_detected",
        "opportunity_observations",
        ["route_id", "detected_at"],
    )
    op.create_index(
        "ix_opportunity_observations_executable_detected",
        "opportunity_observations",
        ["executable", "detected_at"],
    )
    op.create_index(
        "ix_opportunity_observations_accepted_detected",
        "opportunity_observations",
        ["accepted", "detected_at"],
    )
    op.create_index(
        "ix_opportunity_observations_rejection_detected",
        "opportunity_observations",
        ["rejection_reason", "detected_at"],
    )


def downgrade() -> None:
    op.drop_table("opportunity_observations")
    op.drop_table("opportunity_windows")
