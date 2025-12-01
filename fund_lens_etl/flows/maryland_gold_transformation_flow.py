"""
Maryland Gold Transformation Flow

Transforms Maryland Silver layer data into Gold layer (analytics-ready dimensional model).
This integrates Maryland data into the unified gold layer alongside FEC data.

- Contributors: Deduplicate and merge into gold_contributor
- Committees: Merge into gold_committee (using state_committee_id for MD CCF ID)
- Candidates: Merge into gold_candidate (using state_candidate_id)
- Contributions: Create fact records in gold_contribution with FK resolution
"""

import re
from typing import Any

import pandas as pd
from fund_lens_models.gold import (
    GoldCandidate,
    GoldCommittee,
    GoldContribution,
    GoldContributor,
)
from fund_lens_models.silver import (
    SilverMarylandCandidate,
    SilverMarylandCommittee,
    SilverMarylandContribution,
)
from prefect import flow, get_run_logger, task
from prefect.exceptions import MissingContextError
from sqlalchemy import select

from fund_lens_etl.database import get_session

# Retry configuration for transformation tasks
MD_GOLD_RETRY_CONFIG = {
    "retries": 3,
    "retry_delay_seconds": 30,
}
MD_GOLD_TASK_TIMEOUT = 1800  # 30 minutes


def get_logger():
    """Get logger - Prefect if available, otherwise standard logging."""
    try:
        return get_run_logger()
    except MissingContextError:
        import logging

        return logging.getLogger(__name__)


def _normalize_md_contributor_key(
    name: str | None,
    city: str | None,
    state: str | None,
    employer: str | None,
    contributor_type: str | None = None,
) -> str:
    """
    Create a normalized key for Maryland contributor matching.

    For organizations (Business, Political Committee, etc.):
    - Name is normalized (punctuation removed)
    - Location is IGNORED

    For individuals:
    - Full matching on name + city + state + employer

    Args:
        name: Contributor name
        city: Contributor city
        state: Contributor state
        employer: Contributor employer
        contributor_type: Maryland contributor type

    Returns:
        Normalized key string for matching
    """
    # Maryland organization types
    org_types = {
        "Business",
        "Political Committee",
        "Labor Organization",
        "Association",
        "Other",
        "Candidate Loan",
    }

    is_org = contributor_type in org_types if contributor_type else False

    if is_org:
        # Normalize org name
        name_norm = re.sub(r'[,.\-\'"()]', "", name or "")
        name_norm = re.sub(r"\s+", " ", name_norm).upper().strip()
        return f"ORG|{name_norm}"

    # For individuals
    name_norm = (name or "").upper().strip()
    city_norm = (city or "").upper().strip()
    state_norm = (state or "").upper().strip()
    employer_norm = (employer or "").upper().strip()

    return f"{name_norm}|{city_norm}|{state_norm}|{employer_norm}"


def _calculate_md_match_confidence(row: pd.Series) -> float:
    """
    Calculate confidence score for Maryland contributor match.

    Args:
        row: Pandas Series with contributor data

    Returns:
        Confidence score between 0.0 and 1.0
    """
    confidence = 0.0

    if pd.notna(row.get("name")) and row.get("name"):
        confidence += 0.4

    if pd.notna(row.get("city")) and row.get("city"):
        confidence += 0.2
    if pd.notna(row.get("state")) and row.get("state"):
        confidence += 0.2

    if pd.notna(row.get("employer")) and row.get("employer"):
        confidence += 0.1
    if pd.notna(row.get("occupation")) and row.get("occupation"):
        confidence += 0.1

    return min(confidence, 1.0)


@task(
    name="transform_md_contributors_to_gold",
    description="Deduplicate and transform Maryland contributors to gold",
    retries=MD_GOLD_RETRY_CONFIG["retries"],
    retry_delay_seconds=MD_GOLD_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=MD_GOLD_TASK_TIMEOUT,
)
def transform_md_contributors_task() -> dict[str, Any]:
    """
    Transform Maryland silver contributors to gold dimension with deduplication.

    Extracts unique contributors from silver_md_contribution and merges
    into the unified gold_contributor table.

    Returns:
        Dictionary with transformation statistics
    """
    logger = get_logger()
    logger.info("Starting Maryland contributor transformation to gold layer")

    with get_session() as session:
        # Get unique contributors from Maryland contributions
        stmt = select(
            SilverMarylandContribution.contributor_name,
            SilverMarylandContribution.contributor_city,
            SilverMarylandContribution.contributor_state,
            SilverMarylandContribution.contributor_zip,
            SilverMarylandContribution.employer_name,
            SilverMarylandContribution.employer_occupation,
            SilverMarylandContribution.contributor_type,
        ).group_by(
            SilverMarylandContribution.contributor_name,
            SilverMarylandContribution.contributor_city,
            SilverMarylandContribution.contributor_state,
            SilverMarylandContribution.contributor_zip,
            SilverMarylandContribution.employer_name,
            SilverMarylandContribution.employer_occupation,
            SilverMarylandContribution.contributor_type,
        )

        df = pd.read_sql(stmt, session.connection())
        logger.info(f"Loaded {len(df)} unique contributor records from MD silver")

        if df.empty:
            return {
                "total_silver_contributors": 0,
                "total_gold_contributors": 0,
                "new_contributors": 0,
                "deduplication_rate": 0.0,
            }

        # Rename columns for gold schema
        df = df.rename(
            columns={
                "contributor_name": "name",
                "contributor_city": "city",
                "contributor_state": "state",
                "contributor_zip": "zip",
                "employer_name": "employer",
                "employer_occupation": "occupation",
            }
        )

        # Map Maryland contributor_type to FEC-style entity_type
        type_mapping = {
            "Individual": "IND",
            "Business": "ORG",
            "Political Committee": "COM",
            "Labor Organization": "ORG",
            "Association": "ORG",
            "Candidate Loan": "IND",
            "Other": "ORG",
        }
        df["entity_type"] = df["contributor_type"].map(type_mapping).fillna("IND")

        # Create normalized key for deduplication
        df["match_key"] = df.apply(
            lambda r: _normalize_md_contributor_key(
                r["name"], r["city"], r["state"], r["employer"], r["contributor_type"]
            ),
            axis=1,
        )

        # Deduplicate
        df_deduped = df.drop_duplicates(subset=["match_key"], keep="first").copy()
        df_deduped["match_confidence"] = df_deduped.apply(_calculate_md_match_confidence, axis=1)
        df_deduped = df_deduped.drop(columns=["match_key", "contributor_type"])

        dedup_count = len(df) - len(df_deduped)
        logger.info(f"Deduplicated {dedup_count} contributor records")

        # Build lookup of existing contributors
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
            key = _normalize_md_contributor_key(
                contributor.name,
                contributor.city,
                contributor.state,
                contributor.employer,
                None,  # entity_type not used for key in gold
            )
            existing_lookup.add(key)
        logger.info(f"Found {len(existing_lookup)} existing contributors in gold")

        # Create lookup key for new contributors
        df_deduped["lookup_key"] = df_deduped.apply(
            lambda r: _normalize_md_contributor_key(
                r["name"], r["city"], r["state"], r["employer"], None
            ),
            axis=1,
        )

        # Filter to new contributors only
        new_contributors = df_deduped[~df_deduped["lookup_key"].isin(existing_lookup)].copy()
        new_contributors = new_contributors.drop(columns=["lookup_key"])

        logger.info(f"Identified {len(new_contributors)} new contributors to insert")

        # Bulk insert new contributors
        loaded_count = 0
        if not new_contributors.empty:
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
                for _, row in new_contributors.iterrows()
            ]

            session.add_all(contributors)
            session.commit()
            loaded_count = len(contributors)
            logger.info(f"Loaded {loaded_count} new contributors to gold")

        return {
            "total_silver_contributors": len(df),
            "total_gold_contributors": len(df_deduped),
            "new_contributors": loaded_count,
            "deduplication_rate": dedup_count / len(df) if len(df) > 0 else 0.0,
        }


@task(
    name="transform_md_committees_to_gold",
    description="Transform Maryland committees to gold dimension",
    retries=MD_GOLD_RETRY_CONFIG["retries"],
    retry_delay_seconds=MD_GOLD_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=MD_GOLD_TASK_TIMEOUT,
)
def transform_md_committees_task() -> dict[str, Any]:
    """
    Transform Maryland silver committees to gold dimension.

    Uses state_committee_id to store the Maryland CCF ID.

    Returns:
        Dictionary with transformation statistics
    """
    logger = get_logger()
    logger.info("Starting Maryland committee transformation to gold layer")

    with get_session() as session:
        # Get committees not yet in gold (by state_committee_id)
        subquery = (
            select(GoldCommittee.state_committee_id)
            .where(GoldCommittee.state_committee_id == SilverMarylandCommittee.source_ccf_id)
            .exists()
        )
        stmt = select(SilverMarylandCommittee).where(~subquery)

        df = pd.read_sql(stmt, session.connection())
        logger.info(f"Loaded {len(df)} new committee records from MD silver")

        if df.empty:
            return {
                "total_committees": 0,
                "loaded_count": 0,
            }

        # Map committee types to gold schema
        type_mapping = {
            "Candidate": "CANDIDATE",
            "Political Action Committee": "PAC",
            "Party Central Committee": "PARTY",
            "Ballot Issue Committee": "PAC",
            "Independent Expenditure": "SUPER_PAC",
            "Inaugural": "OTHER",
            "Slate": "PAC",
        }

        committees = [
            GoldCommittee(
                name=row["name"],
                committee_type=type_mapping.get(row["committee_type"], "OTHER"),
                state="MD",
                city=None,  # MD data doesn't include city
                candidate_id=None,  # Will be linked later if needed
                state_committee_id=row["source_ccf_id"],
                fec_committee_id=None,
                is_active=row.get("is_active", True),
            )
            for _, row in df.iterrows()
        ]

        session.add_all(committees)
        session.commit()
        loaded_count = len(committees)

        logger.info(f"Loaded {loaded_count} new committees to gold")

        return {
            "total_committees": len(df),
            "loaded_count": loaded_count,
        }


@task(
    name="transform_md_candidates_to_gold",
    description="Transform Maryland candidates to gold dimension",
    retries=MD_GOLD_RETRY_CONFIG["retries"],
    retry_delay_seconds=MD_GOLD_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=MD_GOLD_TASK_TIMEOUT,
)
def transform_md_candidates_task() -> dict[str, Any]:
    """
    Transform Maryland silver candidates to gold dimension.

    Uses state_candidate_id to store the Maryland content hash.

    Returns:
        Dictionary with transformation statistics
    """
    logger = get_logger()
    logger.info("Starting Maryland candidate transformation to gold layer")

    with get_session() as session:
        # Get candidates not yet in gold (by state_candidate_id)
        subquery = (
            select(GoldCandidate.state_candidate_id)
            .where(GoldCandidate.state_candidate_id == SilverMarylandCandidate.source_content_hash)
            .exists()
        )
        stmt = select(SilverMarylandCandidate).where(~subquery)

        df = pd.read_sql(stmt, session.connection())
        logger.info(f"Loaded {len(df)} new candidate records from MD silver")

        if df.empty:
            return {
                "total_candidates": 0,
                "loaded_count": 0,
            }

        # Map Maryland offices to gold schema
        # Maryland has state-level offices like Governor, Attorney General, etc.
        office_mapping = {
            "Governor / Lt. Governor": "GOVERNOR",
            "Attorney General": "ATTORNEY_GENERAL",
            "Comptroller": "COMPTROLLER",
            "U.S. Senator": "SENATE",
            "Representative in Congress": "HOUSE",
            "State Senator": "STATE_SENATE",
            "Delegate": "STATE_HOUSE",
        }

        candidates = []
        for _, row in df.iterrows():
            office = row["office"]
            # Try to map, otherwise use first word uppercased
            mapped_office = office_mapping.get(office)
            if not mapped_office:
                # Use first significant word
                mapped_office = office.split()[0].upper() if office else "OTHER"

            candidates.append(
                GoldCandidate(
                    name=row["name"],
                    office=mapped_office[:20],  # Truncate to fit schema
                    state="MD",
                    district=row.get("district"),
                    party=row.get("party"),
                    is_active=row.get("is_active", True),
                    state_candidate_id=row["source_content_hash"],
                    fec_candidate_id=None,
                )
            )

        session.add_all(candidates)
        session.commit()
        loaded_count = len(candidates)

        logger.info(f"Loaded {loaded_count} new candidates to gold")

        return {
            "total_candidates": len(df),
            "loaded_count": loaded_count,
        }


@task(
    name="transform_md_contributions_to_gold",
    description="Transform Maryland contributions to gold fact table",
    retries=MD_GOLD_RETRY_CONFIG["retries"],
    retry_delay_seconds=MD_GOLD_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=MD_GOLD_TASK_TIMEOUT * 2,  # 1 hour for large datasets
)
def transform_md_contributions_task(
    chunksize: int = 10_000,
) -> dict[str, Any]:
    """
    Transform Maryland silver contributions to gold fact table.

    Creates contribution records with FK resolution to:
    - GoldContributor (resolved by normalized key)
    - GoldCommittee (resolved by state_committee_id = CCF ID)

    Args:
        chunksize: Number of records to process per chunk

    Returns:
        Dictionary with transformation statistics
    """
    logger = get_logger()
    logger.info("Starting Maryland contribution transformation to gold layer")

    # Build FK lookup caches
    with get_session() as cache_session:
        logger.info("Building FK lookup caches...")

        # Cache contributors
        contributor_cache = {}
        for contributor in cache_session.execute(
            select(
                GoldContributor.id,
                GoldContributor.name,
                GoldContributor.city,
                GoldContributor.state,
                GoldContributor.employer,
            )
        ):
            key = _normalize_md_contributor_key(
                contributor.name,
                contributor.city,
                contributor.state,
                contributor.employer,
                None,
            )
            contributor_cache[key] = contributor.id
        logger.info(f"Cached {len(contributor_cache)} contributors")

        # Cache committees (by state_committee_id for MD)
        committee_cache = {}
        for committee in cache_session.execute(
            select(GoldCommittee.id, GoldCommittee.state_committee_id)
        ).all():
            if committee.state_committee_id:
                committee_cache[committee.state_committee_id] = committee.id
        logger.info(f"Cached {len(committee_cache)} committees with state_committee_id")

    # Track contributions inserted this run
    contributions_inserted: set[str] = set()

    # Process in chunks
    total_processed = 0
    loaded_count = 0
    unresolved_contributors = 0
    unresolved_committees = 0
    chunks_processed = 0
    last_id = 0

    while True:
        with get_session() as session:
            # Get contributions not yet in gold
            subquery = (
                select(GoldContribution.source_sub_id)
                .where(
                    GoldContribution.source_system == "MARYLAND",
                    GoldContribution.source_sub_id
                    == SilverMarylandContribution.source_content_hash,
                )
                .exists()
            )
            stmt = select(SilverMarylandContribution).where(~subquery)

            # Cursor-based pagination
            stmt = stmt.where(SilverMarylandContribution.id > last_id)
            stmt = stmt.order_by(SilverMarylandContribution.id).limit(chunksize)

            chunk_df = pd.read_sql(stmt, session.connection())

            if chunk_df.empty:
                break

            chunk_size = len(chunk_df)
            total_processed += chunk_size
            last_id = int(chunk_df["id"].iloc[-1])

            # Resolve contributor FK
            chunk_df["contributor_key"] = chunk_df.apply(
                lambda r: _normalize_md_contributor_key(
                    r["contributor_name"],
                    r["contributor_city"],
                    r["contributor_state"],
                    r["employer_name"],
                    r["contributor_type"],
                ),
                axis=1,
            )
            chunk_df["contributor_id_gold"] = chunk_df["contributor_key"].map(contributor_cache)

            # Resolve committee FK (using CCF ID)
            chunk_df["committee_id_gold"] = chunk_df["committee_ccf_id"].map(committee_cache)

            # Track unresolved
            unresolved_contributors += chunk_df["contributor_id_gold"].isna().sum()
            unresolved_committees += chunk_df["committee_id_gold"].isna().sum()

            # Filter to valid records
            valid_chunk = chunk_df[
                chunk_df["contributor_id_gold"].notna() & chunk_df["committee_id_gold"].notna()
            ].copy()

            # Filter out already inserted
            valid_chunk = valid_chunk[
                ~valid_chunk["source_content_hash"].isin(contributions_inserted)
            ]

            # Insert contributions
            if not valid_chunk.empty:
                contributions = []
                for _, row in valid_chunk.iterrows():
                    # Extract year from filing_period (e.g., "2025 Annual" -> 2025)
                    filing_period = row.get("filing_period", "")
                    election_year = 2024  # Default
                    if filing_period:
                        year_match = re.search(r"(\d{4})", str(filing_period))
                        if year_match:
                            election_year = int(year_match.group(1))

                    contribution = GoldContribution(
                        contributor_id=int(row["contributor_id_gold"]),
                        recipient_committee_id=int(row["committee_id_gold"]),
                        recipient_candidate_id=None,  # MD doesn't have direct candidate link
                        contribution_date=row["contribution_date"],
                        amount=row["contribution_amount"],
                        contribution_type=row.get("contribution_type") or "DIRECT",
                        election_type=None,  # MD doesn't have election type in contribution data
                        source_system="MARYLAND",
                        source_transaction_id=None,  # MD doesn't have transaction IDs
                        source_sub_id=row["source_content_hash"],
                        election_year=election_year,
                        election_cycle=election_year,
                        memo_text=None,
                    )
                    contributions.append(contribution)

                session.add_all(contributions)
                session.commit()
                loaded_count += len(contributions)

                contributions_inserted.update(valid_chunk["source_content_hash"].values)

            chunks_processed += 1

            if chunks_processed % 10 == 0:
                logger.info(
                    f"Progress: {chunks_processed} chunks, "
                    f"{total_processed:,} processed, "
                    f"{loaded_count:,} inserted"
                )

    logger.info(f"Completed: {chunks_processed} chunks, {total_processed:,} contributions")
    logger.info(f"Loaded {loaded_count} new contributions to gold")

    if unresolved_contributors > 0:
        logger.warning(f"Unresolved contributors: {unresolved_contributors}")
    if unresolved_committees > 0:
        logger.warning(f"Unresolved committees: {unresolved_committees}")

    return {
        "total_contributions": total_processed,
        "loaded_count": loaded_count,
        "unresolved_contributors": int(unresolved_contributors),
        "unresolved_committees": int(unresolved_committees),
        "chunks_processed": chunks_processed,
    }


@task(
    name="validate_md_gold_transformation",
    description="Validate Maryland gold layer transformation results",
)
def validate_md_gold_transformation_task(
    contributor_stats: dict[str, Any],
    committee_stats: dict[str, Any],
    candidate_stats: dict[str, Any],
    contribution_stats: dict[str, Any],
) -> dict[str, Any]:
    """
    Validate Maryland gold layer transformation results.

    Args:
        contributor_stats: Statistics from contributor transformation
        committee_stats: Statistics from committee transformation
        candidate_stats: Statistics from candidate transformation
        contribution_stats: Statistics from contribution transformation

    Returns:
        Dictionary with validation results
    """
    logger = get_logger()
    logger.info("Validating Maryland gold layer transformation")

    warnings = []
    errors = []

    # Validate contributors
    if (
        contributor_stats["new_contributors"] == 0
        and contributor_stats["total_silver_contributors"] > 0
    ):
        warnings.append("No new contributors added (may all exist already)")

    # Validate committees
    if committee_stats["loaded_count"] == 0 and committee_stats["total_committees"] > 0:
        warnings.append("No new committees added (may all exist already)")

    # Validate contributions
    total = contribution_stats["total_contributions"]
    if total > 0:
        unresolved_rate = (
            contribution_stats["unresolved_contributors"]
            + contribution_stats["unresolved_committees"]
        ) / total

        if unresolved_rate > 0.1:
            errors.append(f"High FK resolution failure rate: {unresolved_rate:.1%}")
        elif unresolved_rate > 0:
            warnings.append(
                f"Some contributions skipped due to unresolved FKs: {unresolved_rate:.1%}"
            )

    status = "error" if errors else "warning" if warnings else "success"

    if errors:
        for error in errors:
            logger.error(f"  - {error}")
    if warnings:
        for warning in warnings:
            logger.warning(f"  - {warning}")
    if not errors and not warnings:
        logger.info("✓ Validation passed")

    return {
        "validation_passed": len(errors) == 0,
        "status": status,
        "warnings": warnings,
        "errors": errors,
    }


@flow(
    name="maryland_gold_transformation",
    description="Transform Maryland silver layer data to gold layer",
    log_prints=True,
)
def maryland_gold_transformation_flow(
    chunksize: int = 10_000,
) -> dict[str, Any]:
    """
    Main flow to transform Maryland silver layer data to gold layer.

    Execution order:
    1. Transform contributors (with deduplication)
    2. Transform committees
    3. Transform candidates
    4. Transform contributions (with FK resolution)
    5. Validate results

    Args:
        chunksize: Records per chunk for contributions

    Returns:
        Dictionary with comprehensive transformation statistics
    """
    logger = get_logger()
    logger.info("=" * 60)
    logger.info("MARYLAND GOLD TRANSFORMATION FLOW")
    logger.info("=" * 60)

    # Step 1: Transform contributors
    logger.info("\nStep 1: Transforming contributors...")
    contributor_stats = transform_md_contributors_task()
    logger.info(
        f"✓ Contributors: {contributor_stats['new_contributors']} new "
        f"(from {contributor_stats['total_silver_contributors']} silver)"
    )

    # Step 2: Transform committees
    logger.info("\nStep 2: Transforming committees...")
    committee_stats = transform_md_committees_task()
    logger.info(f"✓ Committees: {committee_stats['loaded_count']} loaded")

    # Step 3: Transform candidates
    logger.info("\nStep 3: Transforming candidates...")
    candidate_stats = transform_md_candidates_task()
    logger.info(f"✓ Candidates: {candidate_stats['loaded_count']} loaded")

    # Step 4: Transform contributions
    logger.info("\nStep 4: Transforming contributions...")
    contribution_stats = transform_md_contributions_task(chunksize=chunksize)
    logger.info(f"✓ Contributions: {contribution_stats['loaded_count']} loaded")

    if contribution_stats["unresolved_contributors"] > 0:
        logger.warning(
            f"⚠ Skipped {contribution_stats['unresolved_contributors']} "
            "due to unresolved contributors"
        )
    if contribution_stats["unresolved_committees"] > 0:
        logger.warning(
            f"⚠ Skipped {contribution_stats['unresolved_committees']} "
            "due to unresolved committees"
        )

    # Step 5: Validate
    logger.info("\nStep 5: Validating transformation...")
    validation_stats = validate_md_gold_transformation_task(
        contributor_stats=contributor_stats,
        committee_stats=committee_stats,
        candidate_stats=candidate_stats,
        contribution_stats=contribution_stats,
    )

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("MARYLAND GOLD TRANSFORMATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Contributors:  {contributor_stats['new_contributors']:,} new")
    logger.info(f"Committees:    {committee_stats['loaded_count']:,}")
    logger.info(f"Candidates:    {candidate_stats['loaded_count']:,}")
    logger.info(f"Contributions: {contribution_stats['loaded_count']:,}")
    logger.info("=" * 60)

    if validation_stats["validation_passed"]:
        logger.info("✓ Validation: PASSED")
    else:
        logger.error("✗ Validation: FAILED")

    return {
        "contributor_stats": contributor_stats,
        "committee_stats": committee_stats,
        "candidate_stats": candidate_stats,
        "contribution_stats": contribution_stats,
        "validation_stats": validation_stats,
        "success": validation_stats["validation_passed"],
    }
