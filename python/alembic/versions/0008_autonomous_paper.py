"""Isolated decimal paper accounts, intent, reservations, orders, balanced ledger and audit."""
from pathlib import Path
from alembic import op

revision = "0008_autonomous_paper"
down_revision = "0007_reconciliation_idempotency"
branch_labels = None
depends_on = None


def upgrade() -> None:
    sql = Path(__file__).with_name("0008_autonomous_paper.sql").read_text()
    op.execute(sql)


def downgrade() -> None:
    for table in ("paper_commands", "paper_events", "paper_ledger", "paper_orders",
                  "paper_cycles", "paper_balances", "paper_accounts"):
        op.execute(f"DROP TABLE {table}")
