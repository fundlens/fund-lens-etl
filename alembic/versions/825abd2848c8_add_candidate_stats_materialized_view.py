"""Add candidate stats materialized view and composite index

Revision ID: 825abd2848c8
Revises: d80bab2f7ba1
Create Date: 2025-11-25 13:00:00.000000

This migration adds:
1. A composite index on gold_contribution for faster stats aggregation
2. A materialized view (mv_candidate_stats) with pre-computed candidate statistics
3. Indexes on the materialized view for fast lookups and sorting

The materialized view eliminates the need to aggregate 14M+ contribution rows
on every API request, reducing query time from ~90 seconds to ~3ms.

IMPORTANT: After ETL runs that modify gold_contribution, refresh the view:
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_candidate_stats;
"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = '825abd2848c8'
down_revision: Union[str, Sequence[str], None] = 'a3f2c9d87e14'  # After earmark tracking fields
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create materialized view and indexes for candidate stats."""
    # Add composite index on gold_contribution for stats aggregation queries
    # This helps even without the materialized view for other queries
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_gold_contribution_candidate_stats
        ON gold_contribution (recipient_candidate_id, amount)
        WHERE recipient_candidate_id IS NOT NULL
    """)

    # Create materialized view for pre-computed candidate stats
    # Excludes:
    # - is_earmark_receipt=TRUE records (15E receipt duplicates)
    # - Earmarked contributions (conduit pass-throughs like ACTBLUE/WINRED)
    #   Identified by: transaction_id ending in 'E', or memo containing earmark/conduit keywords
    op.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_candidate_stats AS
        SELECT
            recipient_candidate_id AS candidate_id,
            COUNT(id) AS total_contributions,
            COALESCE(SUM(amount), 0) AS total_amount,
            COUNT(DISTINCT contributor_id) AS unique_contributors,
            COALESCE(AVG(amount), 0) AS avg_contribution
        FROM gold_contribution
        WHERE recipient_candidate_id IS NOT NULL
          AND is_earmark_receipt = FALSE
          -- Exclude earmarked contributions (conduit pass-throughs)
          AND source_transaction_id NOT LIKE '%E'
          AND UPPER(COALESCE(memo_text, '')) NOT LIKE '%EARMARK%'
          AND UPPER(COALESCE(memo_text, '')) NOT LIKE '%CONDUIT%'
          AND UPPER(COALESCE(memo_text, '')) NOT LIKE '%ATTRIBUTION BELOW%'
        GROUP BY recipient_candidate_id
    """)

    # Add unique index on candidate_id for fast joins and CONCURRENTLY refresh
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ix_mv_candidate_stats_candidate_id
        ON mv_candidate_stats (candidate_id)
    """)

    # Add index on total_amount for filtering/sorting by fundraising totals
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_mv_candidate_stats_total_amount
        ON mv_candidate_stats (total_amount DESC)
    """)


def downgrade() -> None:
    """Remove materialized view and indexes."""
    # Drop the materialized view (this also drops its indexes)
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_candidate_stats")

    # Drop the composite index on gold_contribution
    op.execute("DROP INDEX IF EXISTS ix_gold_contribution_candidate_stats")
