"""Phase 16 Rust/Python event stream ledger.

Revision ID: 0006_engine_events
Revises: 0005_micro_live
"""

from alembic import op
import sqlalchemy as sa

revision = "0006_engine_events"
down_revision = "0005_micro_live"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "engine_events",
        sa.Column("event_id", sa.String(length=64), nullable=False),
        sa.Column("stream_id", sa.String(length=64), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("schema_version", sa.BigInteger(), nullable=False),
        sa.Column("occurred_at_ms", sa.BigInteger(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("event_id"),
        sa.UniqueConstraint("stream_id"),
    )
    op.create_index(
        "ix_engine_events_type_time",
        "engine_events",
        ["event_type", "occurred_at_ms"],
    )
    op.create_index(
        "ix_engine_events_source_time",
        "engine_events",
        ["source", "occurred_at_ms"],
    )


def downgrade() -> None:
    op.drop_index("ix_engine_events_source_time", table_name="engine_events")
    op.drop_index("ix_engine_events_type_time", table_name="engine_events")
    op.drop_table("engine_events")
