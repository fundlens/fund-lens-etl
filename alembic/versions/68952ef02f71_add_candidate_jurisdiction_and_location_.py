"""add_candidate_jurisdiction_and_location_columns

Revision ID: 68952ef02f71
Revises: 2acf8b2e7a04
Create Date: 2025-12-01 13:56:06.539629

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '68952ef02f71'
down_revision: Union[str, Sequence[str], None] = '2acf8b2e7a04'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('gold_candidate', sa.Column('jurisdiction_level', sa.String(length=20), nullable=True))
    op.add_column('gold_candidate', sa.Column('county', sa.String(length=100), nullable=True))
    op.add_column('gold_candidate', sa.Column('city', sa.String(length=100), nullable=True))
    op.create_index(op.f('ix_gold_candidate_city'), 'gold_candidate', ['city'], unique=False)
    op.create_index(op.f('ix_gold_candidate_county'), 'gold_candidate', ['county'], unique=False)
    op.create_index(op.f('ix_gold_candidate_jurisdiction_level'), 'gold_candidate', ['jurisdiction_level'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f('ix_gold_candidate_jurisdiction_level'), table_name='gold_candidate')
    op.drop_index(op.f('ix_gold_candidate_county'), table_name='gold_candidate')
    op.drop_index(op.f('ix_gold_candidate_city'), table_name='gold_candidate')
    op.drop_column('gold_candidate', 'city')
    op.drop_column('gold_candidate', 'county')
    op.drop_column('gold_candidate', 'jurisdiction_level')
