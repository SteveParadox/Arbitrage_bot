"""Persist reconciliation identity and payload fingerprint without modifying old records."""

from alembic import op
import sqlalchemy as sa

revision = "0007_reconciliation_idempotency"
down_revision = "0006_engine_events"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "micro_live_cycles", sa.Column("reconciliation_digest", sa.String(64), nullable=True)
    )
    op.add_column(
        "micro_live_cycles", sa.Column("reconciliation_request_id", sa.String(64), nullable=True)
    )
    op.create_unique_constraint(
        "uq_micro_live_reconciliation_request", "micro_live_cycles", ["reconciliation_request_id"]
    )


def downgrade() -> None:
    op.drop_constraint("uq_micro_live_reconciliation_request", "micro_live_cycles", type_="unique")
    op.drop_column("micro_live_cycles", "reconciliation_request_id")
    op.drop_column("micro_live_cycles", "reconciliation_digest")
