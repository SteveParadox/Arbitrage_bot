"""Add remaining lifetime to paper simulation results.

Revision ID: 0003_paper_remaining_lifetime
Revises: 0002_paper_simulation
Create Date: 2026-09-29
"""

from alembic import op
import sqlalchemy as sa

revision = "0003_paper_remaining_lifetime"
down_revision = "0002_paper_simulation"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "paper_simulation_results",
        sa.Column("remaining_lifetime_ms", sa.BigInteger()),
    )


def downgrade() -> None:
    op.drop_column("paper_simulation_results", "remaining_lifetime_ms")
