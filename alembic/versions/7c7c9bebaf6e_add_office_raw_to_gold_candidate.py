"""add_office_raw_to_gold_candidate

Revision ID: 7c7c9bebaf6e
Revises: f9fdf9e02d96
Create Date: 2025-12-01 16:30:15.160333

Adds office_raw column to preserve the original office name from source data
before normalization. This allows filtering by normalized office codes while
still displaying the original office name (e.g., "County Commissioner President"
vs normalized "COUNTY_COMM").
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7c7c9bebaf6e'
down_revision: Union[str, Sequence[str], None] = 'f9fdf9e02d96'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add office_raw column and populate from silver data."""
    op.add_column('gold_candidate', sa.Column('office_raw', sa.String(200), nullable=True))

    # Populate office_raw for MD candidates from silver data
    op.execute("""
        UPDATE gold_candidate gc
        SET office_raw = smc.office
        FROM silver_md_candidate smc
        WHERE gc.state_candidate_id = smc.source_content_hash
    """)

    # For FEC candidates, construct office_raw from normalized office
    # (FEC doesn't have a separate raw value, but we can provide readable names)
    op.execute("""
        UPDATE gold_candidate
        SET office_raw = CASE office
            WHEN 'US_HOUSE' THEN 'U.S. Representative'
            WHEN 'US_SENATE' THEN 'U.S. Senator'
            WHEN 'PRESIDENT' THEN 'President of the United States'
            ELSE office
        END
        WHERE fec_candidate_id IS NOT NULL AND office_raw IS NULL
    """)


def downgrade() -> None:
    """Remove office_raw column."""
    op.drop_column('gold_candidate', 'office_raw')
