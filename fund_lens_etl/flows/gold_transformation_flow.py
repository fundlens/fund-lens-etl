"""
Gold Transformation Flow

Transforms Silver layer data into Gold layer (analytics-ready dimensional model).
- Contributors: Deduplicate and create dimension table
- Committees: Create dimension table
- Candidates: Create dimension table
- Contributions: Create fact table with foreign keys to dimensions
"""

import re
from typing import Any, cast

import pandas as pd
from fund_lens_models.gold import (
    GoldCandidate,
    GoldCommittee,
    GoldContribution,
    GoldContributor,
)
from fund_lens_models.silver import (
    SilverFECCandidate,
    SilverFECCommittee,
    SilverFECContribution,
)
from prefect import flow, get_run_logger, task
from prefect.exceptions import MissingContextError
from sqlalchemy import select, text

from fund_lens_etl.database import get_session

# Retry configuration for transformation tasks
GOLD_RETRY_CONFIG = {
    "retries": 3,
    "retry_delay_seconds": 10,
}
GOLD_TASK_TIMEOUT = 1800  # 30 minutes


def get_logger():
    """Get logger - Prefect if available, otherwise standard logging."""
    try:
        return get_run_logger()
    except MissingContextError:
        import logging

        return logging.getLogger(__name__)


def _normalize_org_name(name: str) -> str:
    """
    Normalize an organization name for matching.

    Removes punctuation, standardizes whitespace, and uppercases.
    This allows matching variations like:
    - "TRUMP NATIONAL COMMITTEE JFC INC." vs "TRUMP NATIONAL COMMITTEE JFC, INC."
    - "ACTBLUE, LLC" vs "ACTBLUE LLC"

    Args:
        name: Organization name

    Returns:
        Normalized name string
    """
    if not name:
        return ""
    # Remove common punctuation (commas, periods, hyphens, apostrophes)
    normalized = re.sub(r'[,.\-\'"()]', "", name)
    # Collapse multiple spaces to single space
    normalized = re.sub(r"\s+", " ", normalized)
    # Uppercase and strip
    return normalized.upper().strip()


def _is_organization(entity_type: str | None) -> bool:
    """
    Check if entity type represents an organization (not an individual).

    Organizations can be safely deduplicated by name alone since
    "WINRED" in Arlington VA is the same as "WINRED" in Arlington VT.

    Args:
        entity_type: FEC entity type code

    Returns:
        True if this is an organization type
    """
    if not entity_type:
        return False
    # These are organization types - NOT individuals
    # IND = Individual, so we exclude it
    return entity_type.upper() in {"PAC", "COM", "ORG", "CCM", "PTY"}


def _normalize_contributor_key(
    name: str | None,
    city: str | None,
    state: str | None,
    employer: str | None,
    entity_type: str | None = None,
) -> str:
    """
    Create a normalized key for contributor matching.

    Used for entity resolution - contributors with the same key are considered
    the same person/entity.

    For organizations (PAC, COM, ORG, etc.):
    - Name is normalized (punctuation removed)
    - Location is IGNORED (same org regardless of city/state variations)

    For individuals:
    - Full matching on name + city + state + employer

    Args:
        name: Contributor name
        city: Contributor city
        state: Contributor state
        employer: Contributor employer
        entity_type: FEC entity type (IND, PAC, COM, ORG, etc.)

    Returns:
        Normalized key string for matching (uppercase, stripped)
    """
    # Check if this is an organization
    if _is_organization(entity_type):
        # For organizations: normalize name, ignore location
        # This merges "WINRED" in VA with "WINRED" in VT (data entry errors)
        # and "TRUMP NATIONAL COMMITTEE JFC INC." with "TRUMP NATIONAL COMMITTEE JFC, INC."
        name_norm = _normalize_org_name(name or "")
        return f"ORG|{name_norm}"

    # For individuals: use full matching including location
    # This prevents merging different "JOHN SMITH" contributors
    name_norm = (name or "").upper().strip()
    city_norm = (city or "").upper().strip()
    state_norm = (state or "").upper().strip()
    employer_norm = (employer or "").upper().strip()

    # Combine into a single key
    return f"{name_norm}|{city_norm}|{state_norm}|{employer_norm}"


def _calculate_match_confidence(row: pd.Series) -> float:
    """
    Calculate confidence score for contributor match (0.0 - 1.0).

    Higher confidence when we have more identifying information.

    Args:
        row: Pandas Series with contributor data

    Returns:
        Confidence score between 0.0 and 1.0
    """
    confidence = 0.0

    # Name is required (if missing, low confidence)
    if pd.notna(row.get("name")) and row.get("name"):
        confidence += 0.4

    # Location adds confidence
    if pd.notna(row.get("city")) and row.get("city"):
        confidence += 0.2
    if pd.notna(row.get("state")) and row.get("state"):
        confidence += 0.2

    # Employer/occupation adds confidence
    if pd.notna(row.get("employer")) and row.get("employer"):
        confidence += 0.1
    if pd.notna(row.get("occupation")) and row.get("occupation"):
        confidence += 0.1

    return min(confidence, 1.0)  # Cap at 1.0


# noinspection PyArgumentEqualDefault
@task(
    name="transform_contributors",
    description="Deduplicate and transform contributors from silver to gold",
    retries=GOLD_RETRY_CONFIG["retries"],
    retry_delay_seconds=GOLD_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=GOLD_TASK_TIMEOUT * 2,  # 1 hour for deduplication
)
def transform_contributors_task(
    state: str | None = None,
    cycle: int | None = None,
    chunksize: int = 50_000,
) -> dict[str, Any]:
    """
    Transform silver contributors to gold dimension with deduplication.

    OPTIMIZED: Uses chunked processing to handle DISTINCT on millions of records.

    Entity resolution strategy:
    - Group by normalized key (name + city + state + employer)
    - Take first occurrence as canonical record
    - Calculate match confidence based on data completeness

    Args:
        state: Unused - kept for API compatibility. Processes all contributors
               who donated to committees in the target state (from silver layer).
        cycle: Optional election cycle filter (e.g., 2026)
        chunksize: Records to process per chunk for DISTINCT (default: 50,000)

    Returns:
        Dictionary with transformation statistics
    """

    logger = get_logger()
    logger.info("Starting contributor transformation to gold layer")

    with get_session() as session:
        # Use database-level GROUP BY instead of DISTINCT for better performance
        # Process in chunks based on contributor_name to manage memory
        stmt = select(
            SilverFECContribution.contributor_name,
            SilverFECContribution.contributor_city,
            SilverFECContribution.contributor_state,
            SilverFECContribution.contributor_zip,
            SilverFECContribution.contributor_employer,
            SilverFECContribution.contributor_occupation,
            SilverFECContribution.entity_type,
        ).group_by(
            SilverFECContribution.contributor_name,
            SilverFECContribution.contributor_city,
            SilverFECContribution.contributor_state,
            SilverFECContribution.contributor_zip,
            SilverFECContribution.contributor_employer,
            SilverFECContribution.contributor_occupation,
            SilverFECContribution.entity_type,
        )

        # Apply filters if provided
        if cycle:
            stmt = stmt.where(SilverFECContribution.election_cycle == cycle)

        # Get count (approximate - this is faster than exact count for large tables)
        logger.info("Fetching unique contributors (this may take a moment)...")
        df = pd.read_sql(stmt, session.connection())
        logger.info(f"Loaded {len(df)} unique contributor records from silver")

        if df.empty:
            logger.warning("No contributors found in silver layer")
            return {
                "total_silver_contributors": 0,
                "total_gold_contributors": 0,
                "deduplication_rate": 0.0,
            }

        # Rename columns for gold schema
        df = df.rename(
            columns={
                "contributor_name": "name",
                "contributor_city": "city",
                "contributor_state": "state",
                "contributor_zip": "zip",
                "contributor_employer": "employer",
                "contributor_occupation": "occupation",
            }
        )

        # Create normalized key for deduplication
        # Pass entity_type to apply different logic for organizations vs individuals
        df["match_key"] = df.apply(
            lambda r: _normalize_contributor_key(
                r["name"], r["city"], r["state"], r["employer"], r["entity_type"]
            ),
            axis=1,
        )

        # Deduplicate: keep first occurrence of each match_key
        df_deduped = df.drop_duplicates(subset=["match_key"], keep="first").copy()

        # Calculate match confidence
        df_deduped["match_confidence"] = df_deduped.apply(_calculate_match_confidence, axis=1)

        # Drop the temporary match_key column
        df_deduped = df_deduped.drop(columns=["match_key"])

        dedup_count = len(df) - len(df_deduped)
        logger.info(f"Deduplicated {dedup_count} contributor records")
        logger.info(f"Unique contributors after deduplication: {len(df_deduped)}")

        # OPTIMIZED: Bulk load to gold layer with chunked processing
        loaded_count = 0
        updated_count = 0
        insert_chunksize = 10_000

        # Get existing contributors using streaming to avoid loading 1M+ rows into DataFrame
        logger.info("Fetching existing contributors from gold layer...")
        existing_lookup = set()
        for contributor in session.execute(
            select(
                GoldContributor.name,
                GoldContributor.city,
                GoldContributor.state,
                GoldContributor.employer,
                GoldContributor.entity_type,
            )
        ):
            # Use same normalized key logic for consistency
            key = _normalize_contributor_key(
                contributor.name,
                contributor.city,
                contributor.state,
                contributor.employer,
                contributor.entity_type,
            )
            existing_lookup.add(key)
        logger.info(f"Found {len(existing_lookup)} existing contributors in gold layer")

        # Create lookup key for deduplicated contributors (same normalization as existing_lookup)
        df_deduped["lookup_key"] = df_deduped.apply(
            lambda r: _normalize_contributor_key(
                r["name"], r["city"], r["state"], r["employer"], r["entity_type"]
            ),
            axis=1,
        )

        # Split into new vs existing
        df_deduped["is_new"] = ~df_deduped["lookup_key"].isin(existing_lookup)
        new_contributors = df_deduped[df_deduped["is_new"]].copy()
        existing_contributors = df_deduped[~df_deduped["is_new"]].copy()

        logger.info(f"Identified {len(new_contributors)} new contributors to insert")
        logger.info(f"Identified {len(existing_contributors)} existing contributors to update")

        # Bulk insert new contributors in chunks
        if not new_contributors.empty:
            new_contributors = new_contributors.drop(columns=["lookup_key", "is_new"])

            total_chunks = (len(new_contributors) + insert_chunksize - 1) // insert_chunksize
            logger.info(f"Inserting new contributors in {total_chunks} chunks...")

            for chunk_idx in range(0, len(new_contributors), insert_chunksize):
                chunk = new_contributors.iloc[chunk_idx : chunk_idx + insert_chunksize]

                # Convert to dict records and create model instances
                contributors = [
                    GoldContributor(
                        name=row["name"],
                        city=row["city"],
                        state=row["state"],
                        zip=row["zip"],
                        employer=row["employer"],
                        occupation=row["occupation"],
                        entity_type=row["entity_type"],
                        match_confidence=row["match_confidence"],
                    )
                    for _, row in chunk.iterrows()
                ]

                session.add_all(contributors)
                session.flush()
                loaded_count += len(contributors)

                chunk_num = (chunk_idx // insert_chunksize) + 1
                logger.info(
                    f"  Inserted chunk {chunk_num}/{total_chunks}: {len(contributors)} contributors"
                )

            session.commit()
            logger.info(f"✓ Loaded {loaded_count} new contributors to gold layer")

        # Note: We skip updating existing contributors for performance
        # Updates would require row-by-row processing which is too slow for 1M+ records
        # If needed, this can be done as a separate maintenance task
        updated_count = 0
        if not existing_contributors.empty:
            logger.info(
                f"Skipping updates for {len(existing_contributors)} existing contributors (performance optimization)"
            )

        # Store stats before cleanup
        total_silver = len(df)
        total_gold = len(df_deduped)
        dedup_rate = dedup_count / len(df) if len(df) > 0 else 0.0

        # Explicit cleanup of large DataFrames to free memory before next task
        del df, df_deduped, new_contributors, existing_contributors, existing_lookup

        return {
            "total_silver_contributors": total_silver,
            "total_gold_contributors": total_gold,
            "deduplication_rate": dedup_rate,
            "loaded_count": loaded_count,
            "updated_count": updated_count,
        }


@task(
    name="transform_committees",
    description="Transform committees from silver to gold dimension",
    retries=GOLD_RETRY_CONFIG["retries"],
    retry_delay_seconds=GOLD_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=GOLD_TASK_TIMEOUT,
)
def transform_committees_task(
    state: str | None = None,
    cycle: int | None = None,
) -> dict[str, Any]:
    """
    Transform silver committees to gold dimension.

    No deduplication needed - committees have unique IDs from FEC.
    This is a straightforward mapping with some enrichment.

    Args:
        state: Optional state filter (e.g., "MD")
        cycle: Optional election cycle filter (e.g., 2026)

    Returns:
        Dictionary with transformation statistics
    """
    logger = get_logger()
    logger.info("Starting committee transformation to gold layer")

    with get_session() as session:
        # OPTIMIZATION: Use NOT EXISTS to only select Silver committees not yet in Gold
        subquery = (
            select(GoldCommittee.fec_committee_id)
            .where(GoldCommittee.fec_committee_id == SilverFECCommittee.source_committee_id)
            .exists()
        )
        stmt = select(SilverFECCommittee).where(~subquery)

        # Apply filters if provided
        if state:
            stmt = stmt.where(SilverFECCommittee.state == state)
        if cycle:
            stmt = stmt.where(SilverFECCommittee.election_cycle == cycle)

        # Load into DataFrame
        df = pd.read_sql(stmt, session.connection())
        logger.info(f"Loaded {len(df)} committee records from silver")

        if df.empty:
            logger.warning("No committees found in silver layer")
            return {
                "total_committees": 0,
                "loaded_count": 0,
                "updated_count": 0,
            }

        # OPTIMIZED: Bulk load to gold layer
        loaded_count = 0
        updated_count = 0

        # NOT EXISTS already filtered out existing committees, so just bulk insert
        committees = [
            GoldCommittee(
                fec_committee_id=row["source_committee_id"],
                name=row["name"],
                committee_type=row.get("committee_type") or "UNKNOWN",
                party=row.get("party"),
                state=row.get("state"),
                city=row.get("city"),
                candidate_id=None,  # Will be set during contribution processing if needed
                is_active=True,
            )
            for _, row in df.iterrows()
        ]

        session.add_all(committees)
        session.commit()
        loaded_count = len(committees)

        logger.info(f"Loaded {loaded_count} new committees to gold layer")

        return {
            "total_committees": len(df),
            "loaded_count": loaded_count,
            "updated_count": updated_count,
        }


@task(
    name="transform_candidates",
    description="Transform candidates from silver to gold dimension",
    retries=GOLD_RETRY_CONFIG["retries"],
    retry_delay_seconds=GOLD_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=GOLD_TASK_TIMEOUT,
)
def transform_candidates_task(
    state: str | None = None,
    cycle: int | None = None,
) -> dict[str, Any]:
    """
    Transform silver candidates to gold dimension.

    No deduplication needed - candidates have unique IDs from FEC.
    This is a straightforward mapping with some enrichment.

    Args:
        state: Optional state filter (e.g., "MD")
        cycle: Optional election cycle filter (e.g., 2026)

    Returns:
        Dictionary with transformation statistics
    """
    logger = get_logger()
    logger.info("Starting candidate transformation to gold layer")

    with get_session() as session:
        # OPTIMIZATION: Use NOT EXISTS to only select Silver candidates not yet in Gold
        subquery = (
            select(GoldCandidate.fec_candidate_id)
            .where(GoldCandidate.fec_candidate_id == SilverFECCandidate.source_candidate_id)
            .exists()
        )
        stmt = select(SilverFECCandidate).where(~subquery)

        # Apply filters if provided
        if state:
            stmt = stmt.where(SilverFECCandidate.state == state)
        if cycle:
            stmt = stmt.where(SilverFECCandidate.election_cycle == cycle)

        # Load into DataFrame
        df = pd.read_sql(stmt, session.connection())
        logger.info(f"Loaded {len(df)} candidate records from silver")

        if df.empty:
            logger.warning("No candidates found in silver layer")
            return {
                "total_candidates": 0,
                "loaded_count": 0,
                "updated_count": 0,
            }

        # OPTIMIZED: Bulk load to gold layer
        loaded_count = 0
        updated_count = 0

        # NOT EXISTS already filtered out existing candidates, so just bulk insert
        candidates = [
            GoldCandidate(
                fec_candidate_id=row["source_candidate_id"],
                name=row["name"],
                office=row["office"],
                state=row.get("state"),
                district=row.get("district"),
                party=row.get("party"),
                is_active=row.get("is_active", True),
            )
            for _, row in df.iterrows()
        ]

        session.add_all(candidates)
        session.commit()
        loaded_count = len(candidates)

        logger.info(f"Loaded {loaded_count} new candidates to gold layer")

        return {
            "total_candidates": len(df),
            "loaded_count": loaded_count,
            "updated_count": updated_count,
        }


@task(
    name="transform_contributions",
    description="Transform contributions from silver to gold fact table with FK resolution",
    retries=GOLD_RETRY_CONFIG["retries"],
    retry_delay_seconds=GOLD_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=86400,  # 24 hours for initial backfill of 13M+ records
)
def transform_contributions_task(
    state: str | None = None,
    cycle: int | None = None,
    chunksize: int = 10_000,
) -> dict[str, Any]:
    """
    Transform silver contributions to gold fact table.

    OPTIMIZED:
    - Uses cursor-based pagination instead of OFFSET to avoid performance degradation
    - Creates new session per chunk to prevent memory bloat
    - Processes in chunks to avoid loading millions of records into memory

    This creates the central fact table with foreign keys to:
    - GoldContributor (resolved by name/city/state/employer)
    - GoldCommittee (resolved by fec_committee_id)
    - GoldCandidate (resolved by fec_candidate_id)

    Args:
        state: Unused - kept for API compatibility. Processes all contributions
               from silver layer (already filtered to target state committees).
        cycle: Optional election cycle filter (e.g., 2026)
        chunksize: Number of records to process per chunk (default: 10,000)

    Returns:
        Dictionary with transformation statistics
    """

    logger = get_logger()
    logger.info("Starting contribution transformation to gold layer")

    # Build FK lookup caches with a separate session
    with get_session() as cache_session:
        logger.info("Building FK lookup caches...")

        # Cache contributors (normalized_key -> id)
        # Use SQLAlchemy streaming instead of pandas to avoid loading 1M+ rows into DataFrame
        contributor_cache = {}
        for contributor in cache_session.execute(
            select(
                GoldContributor.id,
                GoldContributor.name,
                GoldContributor.city,
                GoldContributor.state,
                GoldContributor.employer,
                GoldContributor.entity_type,
            )
        ):
            # Use normalized key for consistent matching with organizations
            key = _normalize_contributor_key(
                contributor.name,
                contributor.city,
                contributor.state,
                contributor.employer,
                contributor.entity_type,
            )
            contributor_cache[key] = contributor.id
        logger.info(f"Cached {len(contributor_cache)} contributors")

        # Cache committees
        committee_cache = {}
        for committee in cache_session.execute(select(GoldCommittee)).scalars().all():
            committee_cache[committee.fec_committee_id] = committee.id
        logger.info(f"Cached {len(committee_cache)} committees")

        # Cache candidates
        candidate_cache = {}
        for candidate in cache_session.execute(select(GoldCandidate)).scalars().all():
            candidate_cache[candidate.fec_candidate_id] = candidate.id
        logger.info(f"Cached {len(candidate_cache)} candidates")

    # Initialize set to track contributions inserted during THIS run
    # (NOT EXISTS filter handles pre-existing contributions, this prevents
    # re-inserting records added in earlier chunks of the same run)
    contributions_inserted_this_run: set[str] = set()

    # Skip expensive count query - just process chunks until empty
    # The NOT EXISTS + COUNT(*) query is too memory intensive on 13M+ rows
    logger.info(f"Processing silver contributions in chunks of {chunksize:,}")

    # Process in chunks using cursor-based pagination
    total_processed = 0
    loaded_count = 0
    updated_count = 0
    unresolved_contributors = 0
    unresolved_committees = 0
    unresolved_candidates = 0
    chunks_processed = 0
    last_id = None  # Cursor for pagination

    while True:
        # OPTIMIZATION: Create new session for each chunk to prevent memory bloat
        with get_session() as session:
            # Build query with NOT EXISTS filter
            subquery = (
                select(GoldContribution.source_sub_id)
                .where(
                    GoldContribution.source_system == "FEC",
                    GoldContribution.source_sub_id == SilverFECContribution.source_sub_id,
                )
                .exists()
            )
            stmt = select(SilverFECContribution).where(~subquery)

            # Apply filters
            if cycle:
                stmt = stmt.where(SilverFECContribution.election_cycle == cycle)

            # OPTIMIZATION: Cursor-based pagination (keyset pagination)
            # This avoids the performance degradation of OFFSET
            if last_id is not None:
                stmt = stmt.where(SilverFECContribution.id > last_id)

            # Order by id for consistent cursor pagination
            stmt = stmt.order_by(SilverFECContribution.id).limit(chunksize)

            # Fetch chunk
            chunk_df = pd.read_sql(stmt, session.connection())

            if chunk_df.empty:
                break

            chunk_size = len(chunk_df)
            total_processed += chunk_size

            # Update cursor to last id in this chunk (convert to Python int)
            last_id = int(chunk_df["id"].iloc[-1])

            # OPTIMIZED: Bulk resolve FKs using cache lookups
            # Create lookup key for contributors (using normalized key for org matching)
            chunk_df["contributor_key"] = chunk_df.apply(
                lambda r: _normalize_contributor_key(
                    r["contributor_name"],
                    r["contributor_city"],
                    r["contributor_state"],
                    r["contributor_employer"],
                    r["entity_type"],
                ),
                axis=1,
            )

            # Resolve FKs using cache
            chunk_df["contributor_id_gold"] = chunk_df["contributor_key"].map(contributor_cache)
            chunk_df["committee_id_gold"] = chunk_df["committee_id"].map(committee_cache)
            chunk_df["candidate_id_gold"] = chunk_df["candidate_id"].map(candidate_cache)

            # Filter out contributions with unresolved required FKs
            unresolved_contributors += chunk_df["contributor_id_gold"].isna().sum()
            unresolved_committees += chunk_df["committee_id_gold"].isna().sum()
            unresolved_candidates += (
                chunk_df["candidate_id"].notna() & chunk_df["candidate_id_gold"].isna()
            ).sum()

            # Keep only rows with resolved contributor and committee (candidate is optional)
            valid_chunk = chunk_df[
                chunk_df["contributor_id_gold"].notna() & chunk_df["committee_id_gold"].notna()
            ].copy()

            # Filter out existing contributions using cache
            valid_chunk = valid_chunk[
                ~valid_chunk["source_sub_id"].isin(contributions_inserted_this_run)
            ]

            # Bulk insert new contributions
            if not valid_chunk.empty:
                # Convert DataFrame to list of dicts
                records = cast(list[dict[str, Any]], valid_chunk.to_dict("records"))

                contributions = []
                for record in records:
                    # Convert NaN to None for nullable fields
                    candidate_id = (
                        record["candidate_id_gold"]
                        if pd.notna(record["candidate_id_gold"])
                        else None
                    )

                    contribution = GoldContribution(
                        contributor_id=record["contributor_id_gold"],
                        recipient_committee_id=record["committee_id_gold"],
                        recipient_candidate_id=candidate_id,
                        contribution_date=record["contribution_date"],
                        amount=record["contribution_amount"],
                        contribution_type=record.get("receipt_type") or "DIRECT",
                        election_type=record.get("election_type"),
                        source_system="FEC",
                        source_transaction_id=record.get("transaction_id"),
                        source_sub_id=record["source_sub_id"],
                        election_year=record["election_cycle"],
                        election_cycle=record["election_cycle"],
                        memo_text=record.get("memo_text"),
                    )
                    contributions.append(contribution)

                session.add_all(contributions)
                session.commit()
                loaded_count += len(contributions)

                # Add to existing set to avoid re-inserting in later chunks
                contributions_inserted_this_run.update(valid_chunk["source_sub_id"].values)

            chunks_processed += 1

            # Log progress every 10 chunks
            if chunks_processed % 10 == 0:
                logger.info(
                    f"Progress: {chunks_processed} chunks, "
                    f"{total_processed:,} processed, "
                    f"{loaded_count:,} inserted"
                )

        # Session is automatically closed at end of 'with' block, preventing memory bloat

    logger.info(f"Completed: {chunks_processed} chunks, {total_processed:,} contributions")
    logger.info(f"Loaded {loaded_count} new contributions to gold layer")

    if unresolved_contributors > 0:
        logger.warning(f"Unresolved contributors: {unresolved_contributors}")
    if unresolved_committees > 0:
        logger.warning(f"Unresolved committees: {unresolved_committees}")
    if unresolved_candidates > 0:
        logger.info(
            f"Unresolved candidates: {unresolved_candidates} (expected for non-candidate committees)"
        )

    return {
        "total_contributions": total_processed,
        "loaded_count": loaded_count,
        "updated_count": updated_count,
        "unresolved_contributors": unresolved_contributors,
        "unresolved_committees": unresolved_committees,
        "unresolved_candidates": unresolved_candidates,
        "chunks_processed": chunks_processed,
    }


@task(
    name="reconcile_earmarked_contributions",
    description="Mark earmark receipt records and link conduit committees",
    retries=GOLD_RETRY_CONFIG["retries"],
    retry_delay_seconds=GOLD_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=GOLD_TASK_TIMEOUT,
)
def reconcile_earmarked_contributions_task() -> dict[str, Any]:
    """
    Reconcile earmarked contribution pairs to prevent double-counting.

    FEC earmarked contributions appear twice:
    1. Receipt by committee (type 15E, transaction_id = 'XXXXX')
    2. Earmark to final recipient (transaction_id = 'XXXXXE')

    This task:
    - Marks the 15E receipt records with is_earmark_receipt=True
    - Sets conduit_committee_id on the earmark records (optional future enhancement)

    Only records with is_earmark_receipt=False should be included in contribution totals.

    Returns:
        Dictionary with reconciliation statistics
    """
    logger = get_logger()
    logger.info("Reconciling earmarked contributions...")

    with get_session() as session:
        # Mark 15E records that have matching earmark records as earmark receipts
        # The matching earmark has transaction_id = base_transaction_id || 'E'
        result = session.execute(
            text("""
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
        )
        marked_count = result.rowcount
        session.commit()

        logger.info(f"✓ Marked {marked_count:,} earmark receipt records")

        # Get stats on earmark distribution
        stats_result = session.execute(
            text("""
            SELECT
                COUNT(*) FILTER (WHERE is_earmark_receipt = TRUE) as earmark_receipts,
                COUNT(*) FILTER (WHERE is_earmark_receipt = FALSE) as canonical_contributions,
                COUNT(*) as total
            FROM gold_contribution
        """)
        ).fetchone()

        earmark_receipts = stats_result[0] if stats_result else 0
        canonical_contributions = stats_result[1] if stats_result else 0
        total = stats_result[2] if stats_result else 0

        logger.info(f"  Earmark receipts (excluded from stats): {earmark_receipts:,}")
        logger.info(f"  Canonical contributions: {canonical_contributions:,}")
        logger.info(f"  Total records: {total:,}")

    return {
        "marked_count": marked_count,
        "earmark_receipts": earmark_receipts,
        "canonical_contributions": canonical_contributions,
        "total_contributions": total,
    }


@task(
    name="validate_gold_transformation",
    description="Validate gold layer transformation results",
    retries=GOLD_RETRY_CONFIG["retries"],
    retry_delay_seconds=GOLD_RETRY_CONFIG["retry_delay_seconds"],
)
def validate_gold_transformation_task(
    contributor_stats: dict[str, Any],
    committee_stats: dict[str, Any],
    candidate_stats: dict[str, Any],
    contribution_stats: dict[str, Any],
) -> dict[str, Any]:
    """
    Validate gold layer transformation results.

    Checks:
    - No excessive data loss from silver to gold
    - Foreign key resolution rates are acceptable
    - Deduplication rates are reasonable
    - All required entities were created

    Args:
        contributor_stats: Statistics from contributor transformation
        committee_stats: Statistics from committee transformation
        candidate_stats: Statistics from candidate transformation
        contribution_stats: Statistics from contribution transformation

    Returns:
        Dictionary with validation results and warnings/errors
    """
    logger = get_logger()
    logger.info("Validating gold layer transformation")

    warnings = []
    errors = []

    # Validate contributors
    if contributor_stats["total_gold_contributors"] == 0:
        errors.append("No contributors created in gold layer")
    else:
        dedup_rate = contributor_stats["deduplication_rate"]
        if dedup_rate > 0.5:
            warnings.append(
                f"High contributor deduplication rate: {dedup_rate:.1%} "
                "(more than 50% duplicates found)"
            )
        logger.info(
            f"✓ Contributors: {contributor_stats['total_gold_contributors']} unique "
            f"(deduplicated {dedup_rate:.1%})"
        )

    # Validate committees
    if committee_stats["total_committees"] == 0:
        errors.append("No committees created in gold layer")
    else:
        logger.info(f"✓ Committees: {committee_stats['total_committees']}")

    # Validate candidates
    if candidate_stats["total_candidates"] == 0:
        warnings.append("No candidates created in gold layer")
    else:
        logger.info(f"✓ Candidates: {candidate_stats['total_candidates']}")

    # Validate contributions
    if contribution_stats["total_contributions"] == 0:
        errors.append("No contributions created in gold layer")
    else:
        total = contribution_stats["total_contributions"]
        loaded = contribution_stats["loaded_count"]
        updated = contribution_stats["updated_count"]

        # Check FK resolution rates
        unresolved_contributors = contribution_stats["unresolved_contributors"]
        unresolved_committees = contribution_stats["unresolved_committees"]
        unresolved_candidates = contribution_stats["unresolved_candidates"]

        contributor_skip_rate = unresolved_contributors / total if total > 0 else 0
        committee_skip_rate = unresolved_committees / total if total > 0 else 0

        if contributor_skip_rate > 0.1:
            errors.append(
                f"High contributor FK resolution failure rate: {contributor_skip_rate:.1%} "
                "(more than 10% of contributions skipped)"
            )
        elif contributor_skip_rate > 0:
            warnings.append(
                f"Some contributions skipped due to unresolved contributors: "
                f"{unresolved_contributors} ({contributor_skip_rate:.1%})"
            )

        if committee_skip_rate > 0.1:
            errors.append(
                f"High committee FK resolution failure rate: {committee_skip_rate:.1%} "
                "(more than 10% of contributions skipped)"
            )
        elif committee_skip_rate > 0:
            warnings.append(
                f"Some contributions skipped due to unresolved committees: "
                f"{unresolved_committees} ({committee_skip_rate:.1%})"
            )

        # Candidate FK failures are expected (not all committees have candidates)
        if unresolved_candidates > 0:
            logger.info(
                f"Contributions without candidate FK: {unresolved_candidates} "
                "(expected for non-candidate committees)"
            )

        logger.info(
            f"✓ Contributions: {loaded + updated} " f"(loaded: {loaded}, updated: {updated})"
        )

    # Log validation results
    if errors:
        logger.error(f"Validation failed with {len(errors)} error(s):")
        for error in errors:
            logger.error(f"  - {error}")

    if warnings:
        logger.warning(f"Validation completed with {len(warnings)} warning(s):")
        for warning in warnings:
            logger.warning(f"  - {warning}")

    if not errors and not warnings:
        logger.info("✓ Validation passed - no errors or warnings")

    return {
        "validation_passed": len(errors) == 0,
        "warnings": warnings,
        "errors": errors,
        "total_warnings": len(warnings),
        "total_errors": len(errors),
    }


@task(
    name="refresh_materialized_views",
    description="Refresh materialized views after gold transformation",
    retries=GOLD_RETRY_CONFIG["retries"],
    retry_delay_seconds=GOLD_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=GOLD_TASK_TIMEOUT,
)
def refresh_materialized_views_task() -> dict[str, Any]:
    """
    Refresh materialized views that depend on gold layer data.

    Currently refreshes:
    - mv_candidate_stats: Pre-computed candidate fundraising statistics
    - mv_contributor_stats: Pre-computed contributor fundraising statistics

    Uses CONCURRENTLY to allow reads during refresh (requires unique index).

    Returns:
        Dictionary with refresh status and timing
    """
    logger = get_logger()
    logger.info("Refreshing materialized views...")

    import time

    results = {}

    def refresh_view(session, view_name: str) -> dict[str, Any]:
        """Refresh a single materialized view with fallback."""
        start_time = time.time()
        try:
            session.execute(text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name}"))
            session.commit()
            elapsed = time.time() - start_time
            logger.info(f"✓ Refreshed {view_name} in {elapsed:.2f}s")
            return {"status": "success", "elapsed_seconds": round(elapsed, 2)}
        except Exception as e:
            # If CONCURRENTLY fails (e.g., no unique index), try without it
            logger.warning(f"CONCURRENTLY refresh failed for {view_name}, trying standard: {e}")
            session.rollback()
            start_time = time.time()
            session.execute(text(f"REFRESH MATERIALIZED VIEW {view_name}"))
            session.commit()
            elapsed = time.time() - start_time
            logger.info(f"✓ Refreshed {view_name} (non-concurrent) in {elapsed:.2f}s")
            return {"status": "success_non_concurrent", "elapsed_seconds": round(elapsed, 2)}

    views_to_refresh = [
        "mv_candidate_stats",
        "mv_contributor_stats",
        "mv_committee_stats",
        "mv_contributor_committee_stats",
        "mv_contributor_candidate_stats",
    ]

    with get_session() as session:
        for view_name in views_to_refresh:
            try:
                results[view_name] = refresh_view(session, view_name)
            except Exception as e:
                logger.error(f"Failed to refresh {view_name}: {e}")
                results[view_name] = {"status": "failed", "error": str(e)}

    return {
        "views_refreshed": list(results.keys()),
        "details": results,
    }


@flow(
    name="gold_transformation_flow",
    description="Transform silver layer data to gold layer (analytics-ready)",
    log_prints=True,
)
def gold_transformation_flow(
    state: str | None = None,
    cycle: int | None = None,
    chunksize: int = 10_000,
) -> dict[str, Any]:
    """
    Main flow to transform silver layer data to gold layer.

    Execution order:
    1. Transform contributors (with deduplication)
    2. Transform committees
    3. Transform candidates
    4. Transform contributions (with FK resolution)
    5. Reconcile earmarked contributions (mark duplicates)
    6. Validate results
    7. Refresh materialized views (for API performance)

    Args:
        state: Optional state filter (e.g., "MD")
        cycle: Optional election cycle filter (e.g., 2026)
        chunksize: Records per chunk for contributions (default: 10000)

    Returns:
        Dictionary with comprehensive transformation statistics
    """
    logger = get_logger()
    logger.info("=" * 60)
    logger.info("GOLD TRANSFORMATION FLOW")
    logger.info("=" * 60)

    if state:
        logger.info(f"State filter: {state}")
    if cycle:
        logger.info(f"Cycle filter: {cycle}")

    # Step 1: Transform contributors (must happen first - needed for FK resolution)
    logger.info("\nStep 1: Transforming contributors...")
    contributor_stats = transform_contributors_task(state=state, cycle=cycle)
    logger.info(
        f"✓ Contributors: {contributor_stats['total_gold_contributors']} unique "
        f"(from {contributor_stats['total_silver_contributors']} silver records)"
    )

    # Step 2: Transform committees (must happen before contributions)
    logger.info("\nStep 2: Transforming committees...")
    committee_stats = transform_committees_task(state=state, cycle=cycle)
    logger.info(
        f"✓ Committees: {committee_stats['total_committees']} "
        f"(loaded: {committee_stats['loaded_count']}, "
        f"updated: {committee_stats['updated_count']})"
    )

    # Step 3: Transform candidates (must happen before contributions)
    logger.info("\nStep 3: Transforming candidates...")
    candidate_stats = transform_candidates_task(state=state, cycle=cycle)
    logger.info(
        f"✓ Candidates: {candidate_stats['total_candidates']} "
        f"(loaded: {candidate_stats['loaded_count']}, "
        f"updated: {candidate_stats['updated_count']})"
    )

    # Step 4: Transform contributions (requires all dimensions to be populated)
    logger.info("\nStep 4: Transforming contributions...")
    contribution_stats = transform_contributions_task(state=state, cycle=cycle, chunksize=chunksize)
    logger.info(
        f"✓ Contributions: {contribution_stats['loaded_count'] + contribution_stats['updated_count']} "
        f"(loaded: {contribution_stats['loaded_count']}, "
        f"updated: {contribution_stats['updated_count']})"
    )

    if contribution_stats["unresolved_contributors"] > 0:
        logger.warning(
            f"⚠ Skipped {contribution_stats['unresolved_contributors']} contributions "
            "due to unresolved contributors"
        )
    if contribution_stats["unresolved_committees"] > 0:
        logger.warning(
            f"⚠ Skipped {contribution_stats['unresolved_committees']} contributions "
            "due to unresolved committees"
        )

    # Step 5: Reconcile earmarked contributions (mark duplicates)
    logger.info("\nStep 5: Reconciling earmarked contributions...")
    earmark_stats = reconcile_earmarked_contributions_task()
    logger.info(
        f"✓ Earmarks: {earmark_stats['marked_count']:,} receipts marked, "
        f"{earmark_stats['canonical_contributions']:,} canonical contributions"
    )

    # Step 6: Validate transformation
    logger.info("\nStep 6: Validating transformation...")
    validation_stats = validate_gold_transformation_task(
        contributor_stats=contributor_stats,
        committee_stats=committee_stats,
        candidate_stats=candidate_stats,
        contribution_stats=contribution_stats,
    )

    # Step 7: Refresh materialized views (for API performance)
    logger.info("\nStep 7: Refreshing materialized views...")
    refresh_stats = refresh_materialized_views_task()
    logger.info(f"✓ Refreshed views: {refresh_stats['views_refreshed']}")

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("GOLD TRANSFORMATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Contributors:  {contributor_stats['total_gold_contributors']:,}")
    logger.info(f"Committees:    {committee_stats['total_committees']:,}")
    logger.info(f"Candidates:    {candidate_stats['total_candidates']:,}")
    logger.info(
        f"Contributions: {contribution_stats['loaded_count'] + contribution_stats['updated_count']:,}"
    )
    logger.info("=" * 60)

    if validation_stats["validation_passed"]:
        logger.info("✓ Validation: PASSED")
    else:
        logger.error(f"✗ Validation: FAILED ({validation_stats['total_errors']} errors)")

    if validation_stats["total_warnings"] > 0:
        logger.warning(f"⚠ Warnings: {validation_stats['total_warnings']}")

    logger.info("=" * 60)

    return {
        "contributor_stats": contributor_stats,
        "committee_stats": committee_stats,
        "candidate_stats": candidate_stats,
        "contribution_stats": contribution_stats,
        "earmark_stats": earmark_stats,
        "validation_stats": validation_stats,
        "refresh_stats": refresh_stats,
        "success": validation_stats["validation_passed"],
    }
