"""add_election_year_tracking_to_gold_candidate

Revision ID: f9fdf9e02d96
Revises: 08b489c39265
Create Date: 2025-12-01 15:54:50.924144

Adds first_election_year and last_election_year columns to track when
candidates have filed for office. This enables:
- Linking historical candidates to their committees
- Tracking candidate career spans
- Filtering for candidates who filed in specific election cycles
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f9fdf9e02d96'
down_revision: Union[str, Sequence[str], None] = '08b489c39265'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add election year tracking columns."""
    op.add_column('gold_candidate', sa.Column('first_election_year', sa.Integer(), nullable=True))
    op.add_column('gold_candidate', sa.Column('last_election_year', sa.Integer(), nullable=True))
    op.create_index('ix_gold_candidate_first_election_year', 'gold_candidate', ['first_election_year'], unique=False)
    op.create_index('ix_gold_candidate_last_election_year', 'gold_candidate', ['last_election_year'], unique=False)

    # Populate for existing Maryland candidates from their election_year
    # (Current MD candidates are from 2024/2026 cycle)
    op.execute("""
        UPDATE gold_candidate
        SET first_election_year = 2024, last_election_year = 2026
        WHERE state_candidate_id IS NOT NULL AND first_election_year IS NULL
    """)

    # Populate for FEC candidates from bronze data
    # election_years is stored as JSON array, need to use json_array_elements_text
    op.execute("""
        UPDATE gold_candidate gc
        SET first_election_year = subq.first_year,
            last_election_year = subq.last_year
        FROM (
            SELECT
                candidate_id,
                MIN(elem::int) as first_year,
                MAX(elem::int) as last_year
            FROM bronze_fec_candidate,
                 LATERAL json_array_elements_text(election_years::json) as elem
            WHERE election_years IS NOT NULL
            GROUP BY candidate_id
        ) subq
        WHERE gc.fec_candidate_id = subq.candidate_id
          AND gc.first_election_year IS NULL
    """)


def downgrade() -> None:
    """Remove election year tracking columns."""
    op.drop_index('ix_gold_candidate_last_election_year', table_name='gold_candidate')
    op.drop_index('ix_gold_candidate_first_election_year', table_name='gold_candidate')
    op.drop_column('gold_candidate', 'last_election_year')
    op.drop_column('gold_candidate', 'first_election_year')
