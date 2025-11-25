"""Add earmark tracking fields to gold_contribution

Revision ID: a3f2c9d87e14
Revises: 825abd2848c8
Create Date: 2025-11-25 14:00:00.000000

This migration adds fields to track earmarked contributions:
- conduit_committee_id: References the conduit (ActBlue, WinRed) used
- is_earmark_receipt: Marks 15E records that should be excluded from stats

Earmarked contributions appear twice in FEC data:
1. Receipt by conduit (type 15E) - marked is_earmark_receipt=True
2. Actual earmark to recipient (transaction_id ends with 'E') - the canonical record

Only the canonical record (is_earmark_receipt=False) should be included in
contribution totals to avoid double-counting.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a3f2c9d87e14'
down_revision: Union[str, Sequence[str], None] = 'd80bab2f7ba1'  # After existing migrations, before mat view
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add earmark tracking columns."""
    # Add conduit_committee_id for tracking which conduit was used
    op.add_column(
        'gold_contribution',
        sa.Column('conduit_committee_id', sa.Integer(), nullable=True)
    )
    op.create_index(
        'ix_gold_contribution_conduit_committee_id',
        'gold_contribution',
        ['conduit_committee_id']
    )

    # Add is_earmark_receipt flag to mark duplicates
    op.add_column(
        'gold_contribution',
        sa.Column('is_earmark_receipt', sa.Boolean(), nullable=False, server_default='false')
    )
    op.create_index(
        'ix_gold_contribution_is_earmark_receipt',
        'gold_contribution',
        ['is_earmark_receipt']
    )


def downgrade() -> None:
    """Remove earmark tracking columns."""
    op.drop_index('ix_gold_contribution_is_earmark_receipt', table_name='gold_contribution')
    op.drop_column('gold_contribution', 'is_earmark_receipt')
    op.drop_index('ix_gold_contribution_conduit_committee_id', table_name='gold_contribution')
    op.drop_column('gold_contribution', 'conduit_committee_id')
