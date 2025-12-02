"""Fix candidate stats to include committee-linked candidates

Revision ID: 2acf8b2e7a04
Revises: a1b2c3d4e5f6
Create Date: 2025-12-01 14:30:00.000000

The previous mv_candidate_stats view only used recipient_candidate_id from
gold_contribution. However, for Maryland (and potentially other state data),
contributions are linked to committees, and candidates are linked to committees
via gold_committee.candidate_id.

This migration updates the view to COALESCE:
1. Direct candidate link (recipient_candidate_id) - used by FEC
2. Committee's candidate link (gold_committee.candidate_id) - used by state data

This ensures candidates show up in stats even when contributions
are only linked via their committee.
"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = '2acf8b2e7a04'
down_revision: Union[str, Sequence[str], None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Common WHERE clause for excluding FEC earmarks while including all state data
FEC_EARMARK_FILTER = """
    (
        -- Include all non-FEC (state-level) contributions
        gc.source_system != 'FEC'
        OR
        -- For FEC data, exclude earmark duplicates
        (
            gc.source_system = 'FEC'
            AND gc.is_earmark_receipt = FALSE
            AND COALESCE(gc.source_transaction_id, '') NOT LIKE '%E'
            AND UPPER(COALESCE(gc.memo_text, '')) NOT LIKE '%EARMARK%'
            AND UPPER(COALESCE(gc.memo_text, '')) NOT LIKE '%CONDUIT%'
            AND UPPER(COALESCE(gc.memo_text, '')) NOT LIKE '%ATTRIBUTION BELOW%'
        )
    )
"""


def upgrade() -> None:
    """Update mv_candidate_stats and mv_contributor_candidate_stats to use committee linkage."""

    # Drop existing views (only the ones we're recreating)
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_contributor_candidate_stats")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_candidate_stats")

    # Recreate mv_candidate_stats with committee-based candidate linkage
    # Uses COALESCE to prefer direct candidate link, but fall back to committee's candidate
    op.execute(f"""
        CREATE MATERIALIZED VIEW mv_candidate_stats AS
        SELECT
            COALESCE(gc.recipient_candidate_id, comm.candidate_id) AS candidate_id,
            COUNT(gc.id) AS total_contributions,
            COALESCE(SUM(gc.amount), 0) AS total_amount,
            COUNT(DISTINCT gc.contributor_id) AS unique_contributors,
            COALESCE(AVG(gc.amount), 0) AS avg_contribution
        FROM gold_contribution gc
        LEFT JOIN gold_committee comm ON comm.id = gc.recipient_committee_id
        WHERE COALESCE(gc.recipient_candidate_id, comm.candidate_id) IS NOT NULL
          AND {FEC_EARMARK_FILTER}
        GROUP BY COALESCE(gc.recipient_candidate_id, comm.candidate_id)
    """)

    op.execute("""
        CREATE UNIQUE INDEX ix_mv_candidate_stats_candidate_id
        ON mv_candidate_stats (candidate_id)
    """)
    op.execute("""
        CREATE INDEX ix_mv_candidate_stats_total_amount
        ON mv_candidate_stats (total_amount DESC)
    """)

    # Recreate mv_contributor_candidate_stats with committee-based candidate linkage
    op.execute(f"""
        CREATE MATERIALIZED VIEW mv_contributor_candidate_stats AS
        SELECT
            gc.contributor_id,
            COALESCE(gc.recipient_candidate_id, comm.candidate_id) AS candidate_id,
            COUNT(gc.id) AS contribution_count,
            COALESCE(SUM(gc.amount), 0) AS total_amount,
            MIN(gc.contribution_date) AS first_contribution_date,
            MAX(gc.contribution_date) AS last_contribution_date
        FROM gold_contribution gc
        LEFT JOIN gold_committee comm ON comm.id = gc.recipient_committee_id
        WHERE COALESCE(gc.recipient_candidate_id, comm.candidate_id) IS NOT NULL
          AND {FEC_EARMARK_FILTER}
        GROUP BY gc.contributor_id, COALESCE(gc.recipient_candidate_id, comm.candidate_id)
    """)

    op.execute("""
        CREATE UNIQUE INDEX ix_mv_contrib_candidate_pk
        ON mv_contributor_candidate_stats (candidate_id, contributor_id)
    """)
    op.execute("""
        CREATE INDEX ix_mv_contrib_candidate_amount
        ON mv_contributor_candidate_stats (candidate_id, total_amount DESC)
    """)
    op.execute("""
        CREATE INDEX ix_mv_contrib_candidate_contributor
        ON mv_contributor_candidate_stats (contributor_id)
    """)


def downgrade() -> None:
    """Revert to previous view definitions."""
    # Drop the updated views
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_contributor_candidate_stats")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_candidate_stats")

    # Note: Previous version used recipient_candidate_id only
    # Would need to recreate from previous migration if needed
