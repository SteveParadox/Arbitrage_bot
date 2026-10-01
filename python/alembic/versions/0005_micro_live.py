"""Add Phase 13 micro-live canary telemetry.

Revision ID: 0005_micro_live
Revises: 0004_live_shadow
Create Date: 2026-10-01
"""

from alembic import op
import sqlalchemy as sa

revision = "0005_micro_live"
down_revision = "0004_live_shadow"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "micro_live_runs",
        sa.Column("id", sa.String(length=64), primary_key=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("base_asset", sa.String(length=32), nullable=False),
        sa.Column("cycle_notional", sa.Numeric(38, 18), nullable=False),
        sa.Column("hard_cycle_cap", sa.Numeric(38, 18), nullable=False),
        sa.Column("manual_execution_required", sa.Boolean(), nullable=False),
        sa.Column(
            "candidates_recorded",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "reconciled_cycles",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )

    op.create_table(
        "micro_live_cycles",
        sa.Column("trade_id", sa.String(length=96), primary_key=True),
        sa.Column(
            "session_id",
            sa.String(length=64),
            sa.ForeignKey("micro_live_runs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("route_id", sa.String(length=255), nullable=False),
        sa.Column("triangle_id", sa.String(length=255), nullable=False),
        sa.Column("base_asset", sa.String(length=32), nullable=False),
        sa.Column("starting_capital", sa.Numeric(38, 18), nullable=False),
        sa.Column("expected_pnl", sa.Numeric(38, 18), nullable=False),
        sa.Column("expected_fees", sa.Numeric(38, 18), nullable=False),
        sa.Column("expected_slippage", sa.Numeric(38, 18), nullable=False),
        sa.Column("expected_slippage_bps", sa.Numeric(20, 8), nullable=False),
        sa.Column("expected_net_edge_bps", sa.Numeric(20, 8), nullable=False),
        sa.Column("fee_bps_per_leg", sa.JSON(), nullable=False),
        sa.Column("detection_leg_prices", sa.JSON(), nullable=False),
        sa.Column("account_balance", sa.Numeric(38, 18), nullable=False),
        sa.Column("account_equity_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("account_exposure_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("manual_execution_required", sa.Boolean(), nullable=False),
        sa.Column("realized_pnl", sa.Numeric(38, 18)),
        sa.Column("prediction_error", sa.Numeric(38, 18)),
        sa.Column("actual_fee_amount_base", sa.Numeric(38, 18)),
        sa.Column("actual_fees_by_currency", sa.JSON()),
        sa.Column("actual_slippage", sa.Numeric(38, 18)),
        sa.Column("actual_slippage_bps", sa.Numeric(20, 8)),
        sa.Column("execution_time_ms", sa.BigInteger()),
        sa.Column("execution_status", sa.String(length=64)),
        sa.Column("notes", sa.String(length=1024)),
        sa.Column("reconciled_at", sa.DateTime(timezone=True)),
        sa.Column("raw_candidate_event", sa.JSON(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ix_micro_live_cycle_session_detected",
        "micro_live_cycles",
        ["session_id", "detected_at"],
    )
    op.create_index(
        "ix_micro_live_cycle_route_detected",
        "micro_live_cycles",
        ["route_id", "detected_at"],
    )
    op.create_index(
        "ix_micro_live_cycle_prediction_error",
        "micro_live_cycles",
        ["session_id", "prediction_error"],
    )


def downgrade() -> None:
    op.drop_table("micro_live_cycles")
    op.drop_table("micro_live_runs")
