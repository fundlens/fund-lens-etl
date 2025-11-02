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
    timeout_seconds=GOLD_TASK_TIMEOUT,
)
def transform_contributors_task(
    state: str | None = None,
    cycle: int | None = None,
) -> dict[str, Any]:
    """
    Transform silver contributors to gold dimension with deduplication.

    Entity resolution strategy:
    - Group by normalized key (name + city + state + employer)
    - Take first occurrence as canonical record
    - Calculate match confidence based on data completeness

    Args:
        state: Unused - kept for API compatibility. Processes all contributors
               who donated to committees in the target state (from silver layer).
        cycle: Optional election cycle filter (e.g., 2026)

    Returns:
        Dictionary with transformation statistics
    """
    logger = get_logger()
    logger.info("Starting contributor transformation to gold layer")

    with get_session() as session:
        # Build query for silver contributions (unique contributors)
        stmt = select(
            SilverFECContribution.contributor_name,
            SilverFECContribution.contributor_city,
            SilverFECContribution.contributor_state,
            SilverFECContribution.contributor_zip,
            SilverFECContribution.contributor_employer,
            SilverFECContribution.contributor_occupation,
            SilverFECContribution.entity_type,
        ).distinct()

        # Apply filters if provided
        # Note: state parameter not used - silver layer already contains contributions
        # to committees from target state. We process all contributors regardless of
        # their state (out-of-state donors are important for analysis).
        if cycle:
            stmt = stmt.where(SilverFECContribution.election_cycle == cycle)

        # Load into DataFrame
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

        # Load to gold layer with UPSERT
        loaded_count = 0
        updated_count = 0

        for _, row in df_deduped.iterrows():
            # Check if contributor already exists (by name + city + state + employer)
            lookup_stmt = select(GoldContributor).where(
                GoldContributor.name == row["name"],
                GoldContributor.city == row["city"],
                GoldContributor.state == row["state"],
                GoldContributor.employer == row["employer"],
            )
            existing = session.execute(lookup_stmt).scalar_one_or_none()

            if existing:
                # Update existing record
                for col, value in row.items():
                    if isinstance(col, str) and col not in ["match_confidence"]:
                        setattr(existing, col, value)
                existing.match_confidence = row["match_confidence"]
                updated_count += 1
            else:
                # Insert new record
                contributor = GoldContributor(
                    name=row["name"],
                    city=row["city"],
                    state=row["state"],
                    zip=row["zip"],
                    employer=row["employer"],
                    occupation=row["occupation"],
                    entity_type=row["entity_type"],
                    match_confidence=row["match_confidence"],
                )
                session.add(contributor)
                loaded_count += 1

        session.commit()

        logger.info(f"Loaded {loaded_count} new contributors to gold layer")
        logger.info(f"Updated {updated_count} existing contributors")

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
        # Build query for silver committees
        stmt = select(SilverFECCommittee)

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

        # Load to gold layer with UPSERT
        loaded_count = 0
        updated_count = 0

        for _, row in df.iterrows():
            # Check if committee already exists (by source_committee_id)
            lookup_stmt = select(GoldCommittee).where(
                GoldCommittee.fec_committee_id == row["source_committee_id"]
            )
            existing = session.execute(lookup_stmt).scalar_one_or_none()

            if existing:
                # Update existing record
                existing.name = row["name"]
                existing.committee_type = row.get("committee_type") or "UNKNOWN"
                existing.party = row.get("party")
                existing.state = row.get("state")
                existing.is_active = True  # Assume active if in silver layer
                updated_count += 1
            else:
                # Insert new record
                committee = GoldCommittee(
                    fec_committee_id=row["source_committee_id"],
                    name=row["name"],
                    committee_type=row.get("committee_type") or "UNKNOWN",
                    party=row.get("party"),
                    state=row.get("state"),
                    is_active=True,
                )
                session.add(committee)
                loaded_count += 1

        session.commit()

        logger.info(f"Loaded {loaded_count} new committees to gold layer")
        logger.info(f"Updated {updated_count} existing committees")

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
        # Build query for silver candidates
        stmt = select(SilverFECCandidate)

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

        # Load to gold layer with UPSERT
        loaded_count = 0
        updated_count = 0

        for _, row in df.iterrows():
            # Check if candidate already exists (by source_candidate_id)
            lookup_stmt = select(GoldCandidate).where(
                GoldCandidate.fec_candidate_id == row["source_candidate_id"]
            )
            existing = session.execute(lookup_stmt).scalar_one_or_none()

            if existing:
                # Update existing record
                existing.name = row["name"]
                existing.office = row["office"]
                existing.state = row.get("state")
                existing.district = row.get("district")
                existing.party = row.get("party")
                existing.is_active = row.get("is_active", True)
                updated_count += 1
            else:
                # Insert new record
                candidate = GoldCandidate(
                    fec_candidate_id=row["source_candidate_id"],
                    name=row["name"],
                    office=row["office"],
                    state=row.get("state"),
                    district=row.get("district"),
                    party=row.get("party"),
                    is_active=row.get("is_active", True),
                )
                session.add(candidate)
                loaded_count += 1

        session.commit()

        logger.info(f"Loaded {loaded_count} new candidates to gold layer")
        logger.info(f"Updated {updated_count} existing candidates")

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
    timeout_seconds=GOLD_TASK_TIMEOUT,
)
def transform_contributions_task(
    state: str | None = None,
    cycle: int | None = None,
) -> dict[str, Any]:
    """
    Transform silver contributions to gold fact table.

    This creates the central fact table with foreign keys to:
    - GoldContributor (resolved by name/city/state/employer)
    - GoldCommittee (resolved by fec_committee_id)
    - GoldCandidate (resolved by fec_candidate_id)

    Args:
        state: Unused - kept for API compatibility. Processes all contributions
               from silver layer (already filtered to target state committees).
        cycle: Optional election cycle filter (e.g., 2026)

    Returns:
        Dictionary with transformation statistics
    """
    logger = get_logger()
    logger.info("Starting contribution transformation to gold layer")

    with get_session() as session:
        # Build query for silver contributions
        stmt = select(SilverFECContribution)

        # Apply filters if provided
        # Note: state parameter not used - silver layer already filtered to target
        # state committees. We process all contributions regardless of contributor state.
        if cycle:
            stmt = stmt.where(SilverFECContribution.election_cycle == cycle)

        # Load into DataFrame
        df = pd.read_sql(stmt, session.connection())
        logger.info(f"Loaded {len(df)} contribution records from silver")

        if df.empty:
            logger.warning("No contributions found in silver layer")
            return {
                "total_contributions": 0,
                "loaded_count": 0,
                "updated_count": 0,
                "unresolved_contributors": 0,
                "unresolved_committees": 0,
                "unresolved_candidates": 0,
            }

        # Load to gold layer with UPSERT and FK resolution
        loaded_count = 0
        updated_count = 0
        unresolved_contributors = 0
        unresolved_committees = 0
        unresolved_candidates = 0

        for _, row in df.iterrows():
            # Resolve contributor FK
            contributor_lookup = select(GoldContributor).where(
                GoldContributor.name == row["contributor_name"],
                GoldContributor.city == row["contributor_city"],
                GoldContributor.state == row["contributor_state"],
                GoldContributor.employer == row["contributor_employer"],
            )
            contributor = session.execute(contributor_lookup).scalar_one_or_none()

            if not contributor:
                logger.warning(f"Contributor not found in gold layer: {row['contributor_name']}")
                unresolved_contributors += 1
                continue  # Skip this contribution

            # Resolve committee FK
            committee_lookup = select(GoldCommittee).where(
                GoldCommittee.fec_committee_id == row["committee_id"]
            )
            committee = session.execute(committee_lookup).scalar_one_or_none()

            if not committee:
                logger.warning(f"Committee not found in gold layer: {row['committee_id']}")
                unresolved_committees += 1
                continue  # Skip this contribution

            # Resolve candidate FK (optional - some committees aren't candidate committees)
            candidate = None
            if pd.notna(row.get("candidate_id")):
                candidate_lookup = select(GoldCandidate).where(
                    GoldCandidate.fec_candidate_id == row["candidate_id"]
                )
                candidate = session.execute(candidate_lookup).scalar_one_or_none()

                if not candidate:
                    logger.debug(f"Candidate not found in gold layer: {row['candidate_id']}")
                    unresolved_candidates += 1
                    # Don't skip - candidate FK is optional

            # Check if contribution already exists (by source_sub_id)
            lookup_stmt = select(GoldContribution).where(
                GoldContribution.source_system == "FEC",
                GoldContribution.source_transaction_id == row["source_sub_id"],
            )
            existing = session.execute(lookup_stmt).scalar_one_or_none()

            if existing:
                # Update existing record
                existing.contributor_id = contributor.id
                existing.recipient_committee_id = committee.id
                existing.recipient_candidate_id = candidate.id if candidate else None
                existing.contribution_date = row["contribution_date"]
                existing.amount = row["contribution_amount"]
                existing.contribution_type = row.get("receipt_type") or "DIRECT"  # Changed
                existing.election_type = row.get("election_type")
                existing.election_year = row.get("election_cycle", 2026)
                existing.election_cycle = row.get("election_cycle", 2026)
                existing.memo_text = row.get("memo_text")
                updated_count += 1
            else:
                # Insert new record
                contribution = GoldContribution(
                    contributor_id=contributor.id,
                    recipient_committee_id=committee.id,
                    recipient_candidate_id=candidate.id if candidate else None,
                    contribution_date=row["contribution_date"],
                    amount=row["contribution_amount"],
                    contribution_type=row.get("receipt_type") or "DIRECT",  # Changed
                    election_type=row.get("election_type"),
                    source_system="FEC",
                    source_transaction_id=row["source_sub_id"],
                    election_year=row.get("election_cycle", 2026),
                    election_cycle=row.get("election_cycle", 2026),
                    memo_text=row.get("memo_text"),
                )
                session.add(contribution)
                loaded_count += 1

        session.commit()

        logger.info(f"Loaded {loaded_count} new contributions to gold layer")
        logger.info(f"Updated {updated_count} existing contributions")

        if unresolved_contributors > 0:
            logger.warning(f"Unresolved contributors: {unresolved_contributors}")
        if unresolved_committees > 0:
            logger.warning(f"Unresolved committees: {unresolved_committees}")
        if unresolved_candidates > 0:
            logger.info(
                f"Unresolved candidates: {unresolved_candidates} (expected for non-candidate committees)"
            )

        return {
            "total_contributions": len(df),
            "loaded_count": loaded_count,
            "updated_count": updated_count,
            "unresolved_contributors": unresolved_contributors,
            "unresolved_committees": unresolved_committees,
            "unresolved_candidates": unresolved_candidates,
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
