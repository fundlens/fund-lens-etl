"""Update materialized views to support state-level data

Revision ID: a1b2c3d4e5f6
Revises: 9e49ab3b53c6
Create Date: 2025-12-01 13:00:00.000000

This migration updates all materialized views to properly handle both
FEC and state-level (Maryland) contribution data.

The previous views had FEC-specific earmark filtering logic that excluded
state data because:
1. source_transaction_id NOT LIKE '%E' - MD contributions have NULL transaction IDs
2. is_earmark_receipt = FALSE - Only applies to FEC data
3. Memo text filters - FEC-specific earmark detection

The updated views apply earmark filtering only to FEC data, allowing
state-level contributions to be included in all statistics.

IMPORTANT: After running this migration, refresh all views:
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_candidate_stats;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_committee_stats;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_contributor_stats;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_contributor_committee_stats;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_contributor_candidate_stats;
"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = '9e49ab3b53c6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Common WHERE clause for excluding FEC earmarks while including all state data
# State data (source_system != 'FEC') is always included
# FEC data is filtered to exclude earmark duplicates
FEC_EARMARK_FILTER = """
    (
        -- Include all non-FEC (state-level) contributions
        source_system != 'FEC'
        OR
        -- For FEC data, exclude earmark duplicates
        (
            source_system = 'FEC'
            AND is_earmark_receipt = FALSE
            AND COALESCE(source_transaction_id, '') NOT LIKE '%E'
            AND UPPER(COALESCE(memo_text, '')) NOT LIKE '%EARMARK%'
            AND UPPER(COALESCE(memo_text, '')) NOT LIKE '%CONDUIT%'
            AND UPPER(COALESCE(memo_text, '')) NOT LIKE '%ATTRIBUTION BELOW%'
        )
    )
"""


def upgrade() -> None:
    """Update materialized views to include state-level data."""

    # Drop existing views (must drop in reverse dependency order)
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_contributor_candidate_stats")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_contributor_committee_stats")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_committee_stats")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_contributor_stats")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_candidate_stats")

    # Recreate mv_candidate_stats with state data support
    op.execute(f"""
        CREATE MATERIALIZED VIEW mv_candidate_stats AS
        SELECT
            recipient_candidate_id AS candidate_id,
            COUNT(id) AS total_contributions,
            COALESCE(SUM(amount), 0) AS total_amount,
            COUNT(DISTINCT contributor_id) AS unique_contributors,
            COALESCE(AVG(amount), 0) AS avg_contribution
        FROM gold_contribution
        WHERE recipient_candidate_id IS NOT NULL
          AND {FEC_EARMARK_FILTER}
        GROUP BY recipient_candidate_id
    """)

    op.execute("""
        CREATE UNIQUE INDEX ix_mv_candidate_stats_candidate_id
        ON mv_candidate_stats (candidate_id)
    """)
    op.execute("""
        CREATE INDEX ix_mv_candidate_stats_total_amount
        ON mv_candidate_stats (total_amount DESC)
    """)

    # Recreate mv_contributor_stats with state data support
    op.execute(f"""
        CREATE MATERIALIZED VIEW mv_contributor_stats AS
        SELECT
            contributor_id,
            COUNT(id) AS total_contributions,
            COALESCE(SUM(amount), 0) AS total_amount,
            COUNT(DISTINCT recipient_committee_id) AS unique_recipients,
            COALESCE(AVG(amount), 0) AS avg_contribution,
            MIN(contribution_date) AS first_contribution_date,
            MAX(contribution_date) AS last_contribution_date
        FROM gold_contribution
        WHERE {FEC_EARMARK_FILTER}
        GROUP BY contributor_id
    """)

    op.execute("""
        CREATE UNIQUE INDEX ix_mv_contributor_stats_contributor_id
        ON mv_contributor_stats (contributor_id)
    """)
    op.execute("""
        CREATE INDEX ix_mv_contributor_stats_total_amount
        ON mv_contributor_stats (total_amount DESC)
    """)
    op.execute("""
        CREATE INDEX ix_mv_contributor_stats_amount_contributions
        ON mv_contributor_stats (total_amount DESC, total_contributions DESC)
    """)

    # Recreate mv_committee_stats with state data support
    op.execute(f"""
        CREATE MATERIALIZED VIEW mv_committee_stats AS
        SELECT
            recipient_committee_id AS committee_id,
            COUNT(id) AS total_contributions,
            COALESCE(SUM(amount), 0) AS total_amount,
            COUNT(DISTINCT contributor_id) AS unique_contributors,
            COALESCE(AVG(amount), 0) AS avg_contribution
        FROM gold_contribution
        WHERE recipient_committee_id IS NOT NULL
          AND {FEC_EARMARK_FILTER}
        GROUP BY recipient_committee_id
    """)

    op.execute("""
        CREATE UNIQUE INDEX ix_mv_committee_stats_committee_id
        ON mv_committee_stats (committee_id)
    """)
    op.execute("""
        CREATE INDEX ix_mv_committee_stats_total_amount
        ON mv_committee_stats (total_amount DESC)
    """)

    # Recreate mv_contributor_committee_stats with state data support
    op.execute(f"""
        CREATE MATERIALIZED VIEW mv_contributor_committee_stats AS
        SELECT
            contributor_id,
            recipient_committee_id AS committee_id,
            COUNT(id) AS contribution_count,
            COALESCE(SUM(amount), 0) AS total_amount,
            MIN(contribution_date) AS first_contribution_date,
            MAX(contribution_date) AS last_contribution_date
        FROM gold_contribution
        WHERE recipient_committee_id IS NOT NULL
          AND {FEC_EARMARK_FILTER}
        GROUP BY contributor_id, recipient_committee_id
    """)

    op.execute("""
        CREATE UNIQUE INDEX ix_mv_contrib_committee_pk
        ON mv_contributor_committee_stats (committee_id, contributor_id)
    """)
    op.execute("""
        CREATE INDEX ix_mv_contrib_committee_amount
        ON mv_contributor_committee_stats (committee_id, total_amount DESC)
    """)
    op.execute("""
        CREATE INDEX ix_mv_contrib_committee_contributor
        ON mv_contributor_committee_stats (contributor_id)
    """)

    # Recreate mv_contributor_candidate_stats with state data support
    op.execute(f"""
        CREATE MATERIALIZED VIEW mv_contributor_candidate_stats AS
        SELECT
            contributor_id,
            recipient_candidate_id AS candidate_id,
            COUNT(id) AS contribution_count,
            COALESCE(SUM(amount), 0) AS total_amount,
            MIN(contribution_date) AS first_contribution_date,
            MAX(contribution_date) AS last_contribution_date
        FROM gold_contribution
        WHERE recipient_candidate_id IS NOT NULL
          AND {FEC_EARMARK_FILTER}
        GROUP BY contributor_id, recipient_candidate_id
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
    """Revert to FEC-only materialized views."""
    # Drop the updated views
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_contributor_candidate_stats")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_contributor_committee_stats")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_committee_stats")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_contributor_stats")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_candidate_stats")

    # Note: To fully revert, would need to recreate the original views
    # from the previous migrations. For simplicity, we just drop them here.
    # Re-run the original migrations if needed.
