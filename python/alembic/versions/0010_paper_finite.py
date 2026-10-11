"""Reject non-finite financial values without silently repairing existing data."""
from alembic import op

revision = "0010_paper_finite"
down_revision = "0009_paper_valuation"
branch_labels = None
depends_on = None

FIELDS = {
    "paper_balances": ("available", "reserved"),
    "paper_cycles": ("starting", "expected_profit", "realized_pnl", "cash_delta"),
    "paper_ledger": ("amount",),
    "paper_valuations": ("amount", "estimated_value"),
}


def upgrade() -> None:
    for table, columns in FIELDS.items():
        for column in columns:
            op.create_check_constraint(f"{table}_{column}_finite", table,
                                       f"{column} IS NULL OR ({column} > '-Infinity'::numeric "
                                       f"AND {column} < 'Infinity'::numeric)")


def downgrade() -> None:
    for table, columns in reversed(tuple(FIELDS.items())):
        for column in reversed(columns):
            op.drop_constraint(f"{table}_{column}_finite", table, type_="check")
