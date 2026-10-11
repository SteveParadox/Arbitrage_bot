"""Timestamped conservative paper inventory valuations, separate from realized P&L."""
from alembic import op

revision = "0009_paper_valuation"
down_revision = "0008_autonomous_paper"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE paper_valuations (
        account_id TEXT NOT NULL REFERENCES paper_accounts(id), asset TEXT NOT NULL,
        amount NUMERIC NOT NULL CHECK(amount>=0), estimated_value NUMERIC,
        details JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY(account_id,asset), CHECK(estimated_value IS NULL OR estimated_value>=0)
    )""")


def downgrade() -> None:
    op.execute("DROP TABLE paper_valuations")
