"""Isolated exchange commitments, intent, evidence, reconciliation and audit outbox."""
from pathlib import Path
from alembic import op

revision = "0011_exchange_execution"
down_revision = "0010_paper_finite"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(Path(__file__).with_suffix(".sql").read_text())


def downgrade():
    for table in ("execution_commands", "execution_events", "execution_reconciliation",
                  "execution_movements", "execution_fills", "execution_orders",
                  "execution_cycles", "execution_accounts"):
        op.execute(f"DROP TABLE {table}")
