"""Add contributor stats materialized views and merge duplicate orgs

Revision ID: c9b4e7f12d38
Revises: 825abd2848c8
Create Date: 2025-11-25 14:30:00.000000

This migration adds:
1. A materialized view (mv_contributor_stats) with pre-computed contributor statistics
2. A materialized view (mv_committee_stats) with pre-computed committee statistics
3. A materialized view (mv_contributor_committee_stats) for contributor-by-committee lookups
4. A materialized view (mv_contributor_candidate_stats) for contributor-by-candidate lookups
5. Indexes on the materialized views for fast lookups and sorting
6. Merges duplicate organization contributors (WINRED, ACTBLUE, etc.)

The materialized views eliminate the need to aggregate 14M+ contribution rows
on every API request, reducing query time from ~23 seconds to ~50ms.

IMPORTANT: After ETL runs that modify gold_contribution, refresh the views:
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_contributor_stats;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_committee_stats;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_contributor_committee_stats;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_contributor_candidate_stats;
"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'c9b4e7f12d38'
down_revision: Union[str, Sequence[str], None] = '825abd2848c8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create materialized view for contributor stats and merge duplicate orgs."""

    # Step 1: Merge duplicate organization contributors
    # This updates contributions to point to the canonical (lowest ID) contributor
    # for each normalized organization name, then deletes the duplicates

    # First, create a temporary table to identify org duplicates
    # Organizations are PAC, COM, ORG, CCM, PTY entity types
    op.execute("""
        CREATE TEMPORARY TABLE org_contributor_mapping AS
        WITH normalized_orgs AS (
            SELECT
                id,
                name,
                entity_type,
                -- Normalize: remove punctuation, collapse whitespace, uppercase
                UPPER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(name, '[,.\\-''\"()]', '', 'g'), '\\s+', ' ', 'g'))) AS normalized_name
            FROM gold_contributor
            WHERE entity_type IN ('PAC', 'COM', 'ORG', 'CCM', 'PTY')
        ),
        canonical_orgs AS (
            -- Pick the lowest ID as the canonical record for each normalized name
            SELECT
                normalized_name,
                MIN(id) AS canonical_id
            FROM normalized_orgs
            GROUP BY normalized_name
            HAVING COUNT(*) > 1  -- Only orgs with duplicates
        )
        SELECT
            n.id AS duplicate_id,
            c.canonical_id
        FROM normalized_orgs n
        JOIN canonical_orgs c ON n.normalized_name = c.normalized_name
        WHERE n.id != c.canonical_id
    """)

    # Update contributions to point to canonical contributors
    op.execute("""
        UPDATE gold_contribution gc
        SET contributor_id = m.canonical_id
        FROM org_contributor_mapping m
        WHERE gc.contributor_id = m.duplicate_id
    """)

    # Delete duplicate contributor records
    op.execute("""
        DELETE FROM gold_contributor
        WHERE id IN (SELECT duplicate_id FROM org_contributor_mapping)
    """)

    # Drop temporary table
    op.execute("DROP TABLE org_contributor_mapping")

    # Step 2: Reconcile earmarked contributions (mark 15E receipts)
    # This marks 15E receipt records that have matching earmark records
    # These should be excluded from contribution totals to avoid double-counting
    op.execute("""
        UPDATE gold_contribution g1
        SET is_earmark_receipt = TRUE
        WHERE g1.contribution_type = '15E'
          AND g1.is_earmark_receipt = FALSE
          AND EXISTS (
              SELECT 1 FROM gold_contribution g2
              WHERE g2.source_transaction_id = g1.source_transaction_id || 'E'
                AND g2.source_system = g1.source_system
          )
    """)

    # Step 3: Create materialized view for pre-computed contributor stats
    # Excludes:
    # - is_earmark_receipt=TRUE records (15E receipt duplicates)
    # - Earmarked contributions (conduit pass-throughs like ACTBLUE/WINRED)
    #   Identified by: transaction_id ending in 'E', or memo containing earmark/conduit keywords
    op.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_contributor_stats AS
        SELECT
            gc.contributor_id,
            COUNT(gc.id) AS total_contributions,
            COALESCE(SUM(gc.amount), 0) AS total_amount,
            COUNT(DISTINCT gc.recipient_committee_id) AS unique_recipients,
            COALESCE(AVG(gc.amount), 0) AS avg_contribution,
            MIN(gc.contribution_date) AS first_contribution_date,
            MAX(gc.contribution_date) AS last_contribution_date
        FROM gold_contribution gc
        WHERE gc.is_earmark_receipt = FALSE
          -- Exclude earmarked contributions (conduit pass-throughs)
          AND gc.source_transaction_id NOT LIKE '%E'
          AND UPPER(COALESCE(gc.memo_text, '')) NOT LIKE '%EARMARK%'
          AND UPPER(COALESCE(gc.memo_text, '')) NOT LIKE '%CONDUIT%'
          AND UPPER(COALESCE(gc.memo_text, '')) NOT LIKE '%ATTRIBUTION BELOW%'
        GROUP BY gc.contributor_id
    """)

    # Add unique index on contributor_id for fast joins and CONCURRENTLY refresh
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ix_mv_contributor_stats_contributor_id
        ON mv_contributor_stats (contributor_id)
    """)

    # Add index on total_amount for sorting by top contributors
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_mv_contributor_stats_total_amount
        ON mv_contributor_stats (total_amount DESC)
    """)

    # Add composite index for filtering + sorting
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_mv_contributor_stats_amount_contributions
        ON mv_contributor_stats (total_amount DESC, total_contributions DESC)
    """)

    # Step 4: Create materialized view for pre-computed committee stats
    # Same exclusions as contributor and candidate stats
    op.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_committee_stats AS
        SELECT
            recipient_committee_id AS committee_id,
            COUNT(id) AS total_contributions,
            COALESCE(SUM(amount), 0) AS total_amount,
            COUNT(DISTINCT contributor_id) AS unique_contributors,
            COALESCE(AVG(amount), 0) AS avg_contribution
        FROM gold_contribution
        WHERE recipient_committee_id IS NOT NULL
          AND is_earmark_receipt = FALSE
          -- Exclude earmarked contributions (conduit pass-throughs)
          AND source_transaction_id NOT LIKE '%E'
          AND UPPER(COALESCE(memo_text, '')) NOT LIKE '%EARMARK%'
          AND UPPER(COALESCE(memo_text, '')) NOT LIKE '%CONDUIT%'
          AND UPPER(COALESCE(memo_text, '')) NOT LIKE '%ATTRIBUTION BELOW%'
        GROUP BY recipient_committee_id
    """)

    # Add unique index on committee_id for fast joins and CONCURRENTLY refresh
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ix_mv_committee_stats_committee_id
        ON mv_committee_stats (committee_id)
    """)

    # Add index on total_amount for sorting
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_mv_committee_stats_total_amount
        ON mv_committee_stats (total_amount DESC)
    """)

    # Step 5: Create materialized view for contributor-by-committee lookups
    # This view pre-aggregates contribution stats per contributor per committee
    # Used by /contributors/by-committee/{id} endpoint for fast pagination
    op.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_contributor_committee_stats AS
        SELECT
            contributor_id,
            recipient_committee_id AS committee_id,
            COUNT(id) AS contribution_count,
            COALESCE(SUM(amount), 0) AS total_amount,
            MIN(contribution_date) AS first_contribution_date,
            MAX(contribution_date) AS last_contribution_date
        FROM gold_contribution
        WHERE recipient_committee_id IS NOT NULL
          AND is_earmark_receipt = FALSE
          -- Exclude earmarked contributions (conduit pass-throughs)
          AND source_transaction_id NOT LIKE '%E'
          AND UPPER(COALESCE(memo_text, '')) NOT LIKE '%EARMARK%'
          AND UPPER(COALESCE(memo_text, '')) NOT LIKE '%CONDUIT%'
          AND UPPER(COALESCE(memo_text, '')) NOT LIKE '%ATTRIBUTION BELOW%'
        GROUP BY contributor_id, recipient_committee_id
    """)

    # Add unique composite index for fast lookups and CONCURRENTLY refresh
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ix_mv_contrib_committee_pk
        ON mv_contributor_committee_stats (committee_id, contributor_id)
    """)

    # Add index for sorting by total_amount within a committee
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_mv_contrib_committee_amount
        ON mv_contributor_committee_stats (committee_id, total_amount DESC)
    """)

    # Add index for contributor lookups (when querying all committees for a contributor)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_mv_contrib_committee_contributor
        ON mv_contributor_committee_stats (contributor_id)
    """)

    # Step 6: Create materialized view for contributor-by-candidate lookups
    # This view pre-aggregates contribution stats per contributor per candidate
    # Used by /contributors/by-candidate/{id} endpoint for fast pagination
    op.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_contributor_candidate_stats AS
        SELECT
            contributor_id,
            recipient_candidate_id AS candidate_id,
            COUNT(id) AS contribution_count,
            COALESCE(SUM(amount), 0) AS total_amount,
            MIN(contribution_date) AS first_contribution_date,
            MAX(contribution_date) AS last_contribution_date
        FROM gold_contribution
        WHERE recipient_candidate_id IS NOT NULL
          AND is_earmark_receipt = FALSE
          -- Exclude earmarked contributions (conduit pass-throughs)
          AND source_transaction_id NOT LIKE '%E'
          AND UPPER(COALESCE(memo_text, '')) NOT LIKE '%EARMARK%'
          AND UPPER(COALESCE(memo_text, '')) NOT LIKE '%CONDUIT%'
          AND UPPER(COALESCE(memo_text, '')) NOT LIKE '%ATTRIBUTION BELOW%'
        GROUP BY contributor_id, recipient_candidate_id
    """)

    # Add unique composite index for fast lookups and CONCURRENTLY refresh
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ix_mv_contrib_candidate_pk
        ON mv_contributor_candidate_stats (candidate_id, contributor_id)
    """)

    # Add index for sorting by total_amount within a candidate
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_mv_contrib_candidate_amount
        ON mv_contributor_candidate_stats (candidate_id, total_amount DESC)
    """)

    # Add index for contributor lookups (when querying all candidates for a contributor)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_mv_contrib_candidate_contributor
        ON mv_contributor_candidate_stats (contributor_id)
    """)


def downgrade() -> None:
    """Remove materialized views (cannot restore merged contributors)."""
    # Drop the materialized views (this also drops their indexes)
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_contributor_candidate_stats")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_contributor_committee_stats")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_committee_stats")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_contributor_stats")

    # Note: Cannot restore merged contributors - would need to re-run ETL
