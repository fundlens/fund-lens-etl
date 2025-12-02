"""rename_county_city_to_office_county_office_city

Revision ID: 08b489c39265
Revises: 68952ef02f71
Create Date: 2025-12-01 15:03:58.281448

Renames county -> office_county and city -> office_city in gold_candidate
to clarify that these columns indicate the office location (for local
candidates), not the candidate's address.

Also clears these values for non-COUNTY and non-CITY jurisdiction candidates
since STATE and FEDERAL candidates shouldn't have office_county/office_city values.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '08b489c39265'
down_revision: Union[str, Sequence[str], None] = '68952ef02f71'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Rename columns and clear incorrect values."""
    # Rename columns
    op.alter_column('gold_candidate', 'county', new_column_name='office_county')
    op.alter_column('gold_candidate', 'city', new_column_name='office_city')

    # Rename indexes
    op.drop_index('ix_gold_candidate_county', table_name='gold_candidate')
    op.drop_index('ix_gold_candidate_city', table_name='gold_candidate')
    op.create_index('ix_gold_candidate_office_county', 'gold_candidate', ['office_county'], unique=False)
    op.create_index('ix_gold_candidate_office_city', 'gold_candidate', ['office_city'], unique=False)

    # Clear office_county for non-COUNTY candidates (STATE, FEDERAL should not have this)
    op.execute("""
        UPDATE gold_candidate
        SET office_county = NULL
        WHERE jurisdiction_level NOT IN ('COUNTY') OR jurisdiction_level IS NULL
    """)

    # Clear office_city for non-CITY candidates
    op.execute("""
        UPDATE gold_candidate
        SET office_city = NULL
        WHERE jurisdiction_level NOT IN ('CITY') OR jurisdiction_level IS NULL
    """)


def downgrade() -> None:
    """Revert column renames."""
    # Rename indexes back
    op.drop_index('ix_gold_candidate_office_county', table_name='gold_candidate')
    op.drop_index('ix_gold_candidate_office_city', table_name='gold_candidate')
    op.create_index('ix_gold_candidate_county', 'gold_candidate', ['office_county'], unique=False)
    op.create_index('ix_gold_candidate_city', 'gold_candidate', ['office_city'], unique=False)

    # Rename columns back
    op.alter_column('gold_candidate', 'office_county', new_column_name='county')
    op.alter_column('gold_candidate', 'office_city', new_column_name='city')
