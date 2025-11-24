"""
Gold Transformation Flow

Transforms Silver layer data into Gold layer (analytics-ready dimensional model).
- Contributors: Deduplicate and create dimension table
- Committees: Create dimension table
- Candidates: Create dimension table
- Contributions: Create fact table with foreign keys to dimensions
"""

from typing import Any

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
from sqlalchemy import select

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


def _normalize_contributor_key(
    name: str | None,
    city: str | None,
    state: str | None,
    employer: str | None,
) -> str:
    """
    Create a normalized key for contributor matching.

    Used for entity resolution - contributors with the same key are considered
    the same person/entity.

    Args:
        name: Contributor name
        city: Contributor city
        state: Contributor state
        employer: Contributor employer

    Returns:
        Normalized key string for matching (uppercase, stripped)
    """
    # Normalize each component (uppercase, strip whitespace, handle nulls)
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
        df["match_key"] = df.apply(
            lambda r: _normalize_contributor_key(r["name"], r["city"], r["state"], r["employer"]),
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

        # Get existing contributors in bulk to avoid repeated queries
        logger.info("Fetching existing contributors from gold layer...")
        existing_contributors_df = pd.read_sql(
            select(
                GoldContributor.id,
                GoldContributor.name,
                GoldContributor.city,
                GoldContributor.state,
                GoldContributor.employer,
            ),
            session.connection(),
        )

        # Create lookup key for existing contributors
        if not existing_contributors_df.empty:
            existing_contributors_df["lookup_key"] = existing_contributors_df.apply(
                lambda r: f"{r['name']}|{r['city']}|{r['state']}|{r['employer']}",
                axis=1,
            )
            existing_lookup = set(existing_contributors_df["lookup_key"].values)
            logger.info(f"Found {len(existing_lookup)} existing contributors in gold layer")
        else:
            existing_lookup = set()
            logger.info("No existing contributors found in gold layer")

        # Create lookup key for deduplicated contributors
        df_deduped["lookup_key"] = df_deduped.apply(
            lambda r: f"{r['name']}|{r['city']}|{r['state']}|{r['employer']}", axis=1
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

        return {
            "total_silver_contributors": len(df),
            "total_gold_contributors": len(df_deduped),
            "deduplication_rate": dedup_count / len(df) if len(df) > 0 else 0.0,
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
    timeout_seconds=GOLD_TASK_TIMEOUT * 4,  # 2 hours for large datasets
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
    from sqlalchemy import func

    logger = get_logger()
    logger.info("Starting contribution transformation to gold layer")

    # Build FK lookup caches with a separate session
    with get_session() as cache_session:
        logger.info("Building FK lookup caches...")

        # Cache contributors (name|city|state|employer -> id)
        contributor_cache = {}
        contributor_df = pd.read_sql(
            select(
                GoldContributor.id,
                GoldContributor.name,
                GoldContributor.city,
                GoldContributor.state,
                GoldContributor.employer,
            ),
            cache_session.connection(),
        )
        for _, row in contributor_df.iterrows():
            key = f"{row['name']}|{row['city']}|{row['state']}|{row['employer']}"
            contributor_cache[key] = row["id"]
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

        # Cache existing contributions (source_sub_id -> True for dedup check)
        existing_contributions = set()
        existing_df = pd.read_sql(
            select(GoldContribution.source_sub_id).where(GoldContribution.source_system == "FEC"),
            cache_session.connection(),
        )
        existing_contributions = set(existing_df["source_sub_id"].values)
        logger.info(f"Cached {len(existing_contributions)} existing contributions for dedup")

    # Get count with a separate session
    with get_session() as count_session:
        # Build query for silver contributions
        # OPTIMIZATION: Use NOT EXISTS to only select Silver records not yet in Gold
        # This prevents re-transforming millions of already-processed records
        subquery = (
            select(GoldContribution.source_sub_id)
            .where(
                GoldContribution.source_system == "FEC",
                GoldContribution.source_sub_id == SilverFECContribution.source_sub_id,
            )
            .exists()
        )

        # Get total count for progress tracking (only count records not yet in Gold)
        count_stmt = select(func.count()).select_from(SilverFECContribution).where(~subquery)
        if cycle:
            count_stmt = count_stmt.where(SilverFECContribution.election_cycle == cycle)

        total_count = count_session.execute(count_stmt).scalar()
        logger.info(f"Processing {total_count:,} silver contributions in chunks of {chunksize:,}")

        if total_count == 0:
            logger.warning("No contributions found in silver layer")
            return {
                "total_contributions": 0,
                "loaded_count": 0,
                "updated_count": 0,
                "unresolved_contributors": 0,
                "unresolved_committees": 0,
                "unresolved_candidates": 0,
                "chunks_processed": 0,
            }

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

            # Update cursor to last id in this chunk
            last_id = chunk_df["id"].iloc[-1]

            # OPTIMIZED: Bulk resolve FKs using cache lookups
            # Create lookup key for contributors
            chunk_df["contributor_key"] = chunk_df.apply(
                lambda r: f"{r['contributor_name']}|{r['contributor_city']}|{r['contributor_state']}|{r['contributor_employer']}",
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
            valid_chunk = valid_chunk[~valid_chunk["source_sub_id"].isin(existing_contributions)]

            # Bulk insert new contributions
            if not valid_chunk.empty:
                contributions = []
                for _, row in valid_chunk.iterrows():
                    # Convert numpy types to Python native types for SQLAlchemy
                    # Use .item() to convert numpy scalars to Python scalars

                    # Handle contribution_date - could be pandas Timestamp or Python date
                    contrib_date = row["contribution_date"]
                    if pd.notna(contrib_date):
                        if hasattr(contrib_date, "to_pydatetime"):
                            contrib_date = contrib_date.to_pydatetime().date()
                        # else it's already a Python date object
                    else:
                        contrib_date = None

                    # Handle candidate_id (optional FK)
                    candidate_id_val = None
                    if pd.notna(row["candidate_id_gold"]):
                        candidate_id_val = (
                            int(row["candidate_id_gold"].item())
                            if hasattr(row["candidate_id_gold"], "item")
                            else int(row["candidate_id_gold"])
                        )

                    # Handle election cycle
                    election_cycle_val = row.get("election_cycle", 2026)
                    if hasattr(election_cycle_val, "item"):
                        election_cycle_val = int(election_cycle_val.item())
                    else:
                        election_cycle_val = int(election_cycle_val)

                    contribution = GoldContribution(
                        contributor_id=int(row["contributor_id_gold"].item())
                        if hasattr(row["contributor_id_gold"], "item")
                        else int(row["contributor_id_gold"]),
                        recipient_committee_id=int(row["committee_id_gold"].item())
                        if hasattr(row["committee_id_gold"], "item")
                        else int(row["committee_id_gold"]),
                        recipient_candidate_id=candidate_id_val,
                        contribution_date=contrib_date,
                        amount=float(row["contribution_amount"].item())
                        if hasattr(row["contribution_amount"], "item")
                        else float(row["contribution_amount"]),
                        contribution_type=str(row.get("receipt_type"))
                        if pd.notna(row.get("receipt_type"))
                        else "DIRECT",
                        election_type=str(row.get("election_type"))
                        if pd.notna(row.get("election_type"))
                        else None,
                        source_system="FEC",
                        source_transaction_id=str(row["transaction_id"])
                        if pd.notna(row["transaction_id"])
                        else None,
                        source_sub_id=str(row["source_sub_id"]),
                        election_year=election_cycle_val,
                        election_cycle=election_cycle_val,
                        memo_text=str(row.get("memo_text"))
                        if pd.notna(row.get("memo_text"))
                        else None,
                    )
                    contributions.append(contribution)

                session.add_all(contributions)
                session.commit()
                loaded_count += len(contributions)

                # Add to existing set to avoid re-inserting in later chunks
                existing_contributions.update(valid_chunk["source_sub_id"].values)

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


@flow(
    name="gold_transformation_flow",
    description="Transform silver layer data to gold layer (analytics-ready)",
    log_prints=True,
)
def gold_transformation_flow(
    state: str | None = None,
    cycle: int | None = None,
) -> dict[str, Any]:
    """
    Main flow to transform silver layer data to gold layer.

    Execution order:
    1. Transform contributors (with deduplication)
    2. Transform committees
    3. Transform candidates
    4. Transform contributions (with FK resolution)
    5. Validate results

    Args:
        state: Optional state filter (e.g., "MD")
        cycle: Optional election cycle filter (e.g., 2026)

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
    contribution_stats = transform_contributions_task(state=state, cycle=cycle)
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

    # Step 5: Validate transformation
    logger.info("\nStep 5: Validating transformation...")
    validation_stats = validate_gold_transformation_task(
        contributor_stats=contributor_stats,
        committee_stats=committee_stats,
        candidate_stats=candidate_stats,
        contribution_stats=contribution_stats,
    )

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
        "validation_stats": validation_stats,
        "success": validation_stats["validation_passed"],
    }
