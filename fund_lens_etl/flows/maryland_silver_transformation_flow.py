"""
Maryland Silver Transformation Flow

Transforms Maryland Bronze layer data into Silver layer (cleaned and enriched).
- Committees: Parse dates, determine active status
- Candidates: Parse names, link to committees
- Contributions: Parse dates/amounts, parse addresses, enrich with committee data
"""

import math
from typing import Any

import pandas as pd
from fund_lens_models.bronze import (
    BronzeMarylandCandidate,
    BronzeMarylandCommittee,
    BronzeMarylandContribution,
)
from fund_lens_models.silver import (
    SilverMarylandCandidate,
    SilverMarylandCommittee,
    SilverMarylandContribution,
)
from prefect import flow, task
from sqlalchemy import select

from fund_lens_etl.database import get_session
from fund_lens_etl.loaders.silver import (
    SilverMarylandCandidateLoader,
    SilverMarylandCommitteeLoader,
    SilverMarylandContributionLoader,
)
from fund_lens_etl.transformers import (
    BronzeToSilverMarylandCandidateTransformer,
    BronzeToSilverMarylandCommitteeTransformer,
    BronzeToSilverMarylandContributionTransformer,
)


def clean_nan_values(data_dict: dict[str, Any]) -> dict[str, Any]:
    """
    Replace NaN values with None for database insertion.

    Args:
        data_dict: Dictionary potentially containing NaN values

    Returns:
        Dictionary with NaN values replaced by None
    """
    return {
        k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in data_dict.items()
    }


# Retry configuration for transformation tasks
MD_SILVER_RETRY_CONFIG = {
    "retries": 3,
    "retry_delay_seconds": 30,
}
MD_SILVER_TASK_TIMEOUT = 1800  # 30 minutes


@task(
    name="transform_md_committees",
    retries=MD_SILVER_RETRY_CONFIG["retries"],
    retry_delay_seconds=MD_SILVER_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=MD_SILVER_TASK_TIMEOUT,
)
def transform_md_committees_task() -> dict[str, Any]:
    """
    Transform Bronze Maryland committees to Silver.

    Returns:
        Dict with transformation stats
    """
    from prefect import get_run_logger

    logger = get_run_logger()

    with get_session() as session:
        # Get Bronze committees not yet in Silver
        subquery = (
            select(SilverMarylandCommittee.source_ccf_id)
            .where(SilverMarylandCommittee.source_ccf_id == BronzeMarylandCommittee.ccf_id)
            .exists()
        )
        stmt = select(BronzeMarylandCommittee).where(~subquery)
        bronze_committees = session.execute(stmt).scalars().all()

        if not bronze_committees:
            logger.info("No new committees to transform")
            return {
                "bronze_records": 0,
                "silver_records": 0,
                "skipped": 0,
            }

        logger.info(f"Transforming {len(bronze_committees)} new committees")

        # Get column names from Bronze model
        exclude_cols = {"created_at", "updated_at", "ingestion_timestamp", "source_system"}
        bronze_cols = [
            col.name
            for col in BronzeMarylandCommittee.__table__.columns.values()
            if col.name not in exclude_cols and col.name != "id"
        ]

        # Convert to DataFrame
        bronze_df = pd.DataFrame(
            [{col: getattr(c, col) for col in bronze_cols} for c in bronze_committees]
        )

        # Transform
        transformer = BronzeToSilverMarylandCommitteeTransformer()
        silver_df = transformer.transform(bronze_df)

        # Load to Silver
        loader = SilverMarylandCommitteeLoader()
        records_loaded = loader.load(session, silver_df)

        logger.info(f"Loaded {records_loaded} committees to silver")

        return {
            "bronze_records": len(bronze_committees),
            "silver_records": records_loaded,
            "skipped": len(bronze_committees) - len(silver_df),
        }


@task(
    name="transform_md_candidates",
    retries=MD_SILVER_RETRY_CONFIG["retries"],
    retry_delay_seconds=MD_SILVER_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=MD_SILVER_TASK_TIMEOUT,
)
def transform_md_candidates_task(
    election_year: int | None = None,
) -> dict[str, Any]:
    """
    Transform Bronze Maryland candidates to Silver.

    Args:
        election_year: Optional election year filter

    Returns:
        Dict with transformation stats
    """
    from prefect import get_run_logger

    logger = get_run_logger()

    with get_session() as session:
        # Get Bronze candidates not yet in Silver
        subquery = (
            select(SilverMarylandCandidate.source_content_hash)
            .where(
                SilverMarylandCandidate.source_content_hash == BronzeMarylandCandidate.content_hash
            )
            .exists()
        )
        stmt = select(BronzeMarylandCandidate).where(~subquery)

        if election_year:
            stmt = stmt.where(BronzeMarylandCandidate.election_year == election_year)

        bronze_candidates = session.execute(stmt).scalars().all()

        if not bronze_candidates:
            logger.info("No new candidates to transform")
            return {
                "bronze_records": 0,
                "silver_records": 0,
                "skipped": 0,
            }

        logger.info(f"Transforming {len(bronze_candidates)} new candidates")

        # Get column names from Bronze model
        exclude_cols = {"created_at", "updated_at", "ingestion_timestamp", "source_system"}
        bronze_cols = [
            col.name
            for col in BronzeMarylandCandidate.__table__.columns.values()
            if col.name not in exclude_cols and col.name != "id"
        ]

        # Convert to DataFrame
        bronze_df = pd.DataFrame(
            [{col: getattr(c, col) for col in bronze_cols} for c in bronze_candidates]
        )

        # Transform (with session for committee enrichment)
        transformer = BronzeToSilverMarylandCandidateTransformer(session=session)
        silver_df = transformer.transform(bronze_df)

        # Load to Silver
        loader = SilverMarylandCandidateLoader()
        records_loaded = loader.load(session, silver_df)

        logger.info(f"Loaded {records_loaded} candidates to silver")

        return {
            "bronze_records": len(bronze_candidates),
            "silver_records": records_loaded,
            "skipped": len(bronze_candidates) - len(silver_df),
        }


@task(
    name="transform_md_contributions",
    retries=MD_SILVER_RETRY_CONFIG["retries"],
    retry_delay_seconds=MD_SILVER_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=MD_SILVER_TASK_TIMEOUT * 2,  # 1 hour for large datasets
)
def transform_md_contributions_task(
    filing_year: int | None = None,
    chunksize: int = 10_000,
) -> dict[str, Any]:
    """
    Transform Bronze Maryland contributions to Silver with committee enrichment.

    Uses chunked processing for large datasets.

    Args:
        filing_year: Optional filing year filter (from filing_period)
        chunksize: Number of records to process per chunk

    Returns:
        Dict with transformation stats
    """
    from prefect import get_run_logger
    from sqlalchemy import func

    logger = get_run_logger()

    # Get count of records to process
    with get_session() as count_session:
        subquery = (
            select(SilverMarylandContribution.source_content_hash)
            .where(
                SilverMarylandContribution.source_content_hash
                == BronzeMarylandContribution.content_hash
            )
            .exists()
        )
        count_stmt = select(func.count()).select_from(BronzeMarylandContribution).where(~subquery)
        total_count = count_session.execute(count_stmt).scalar()

        if total_count == 0:
            logger.info("No new contributions to transform")
            return {
                "bronze_records": 0,
                "silver_records": 0,
                "skipped": 0,
                "chunks_processed": 0,
            }

        logger.info(f"Processing {total_count:,} bronze contributions in chunks of {chunksize:,}")

    # Get column names from Bronze model
    exclude_cols = {"created_at", "updated_at", "ingestion_timestamp", "source_system"}
    bronze_cols = [
        col.name
        for col in BronzeMarylandContribution.__table__.columns.values()
        if col.name not in exclude_cols and col.name != "id"
    ]

    # Process in chunks using cursor-based pagination
    total_bronze = 0
    total_silver = 0
    chunks_processed = 0
    last_id = 0

    while True:
        with get_session() as session:
            # Build query with NOT EXISTS filter
            subquery = (
                select(SilverMarylandContribution.source_content_hash)
                .where(
                    SilverMarylandContribution.source_content_hash
                    == BronzeMarylandContribution.content_hash
                )
                .exists()
            )
            stmt = select(BronzeMarylandContribution).where(~subquery)

            # Cursor-based pagination
            stmt = stmt.where(BronzeMarylandContribution.id > last_id)
            stmt = stmt.order_by(BronzeMarylandContribution.id).limit(chunksize)

            bronze_chunk = session.execute(stmt).scalars().all()

            if not bronze_chunk:
                break

            chunk_size = len(bronze_chunk)
            total_bronze += chunk_size
            last_id = bronze_chunk[-1].id

            # Convert to DataFrame
            bronze_df = pd.DataFrame(
                [{col: getattr(c, col) for col in bronze_cols} for c in bronze_chunk]
            )

            # Transform (with session for committee enrichment)
            transformer = BronzeToSilverMarylandContributionTransformer(session=session)
            silver_df = transformer.transform(bronze_df)

            # Load to Silver
            loader = SilverMarylandContributionLoader()
            records_loaded = loader.load(session, silver_df)
            total_silver += records_loaded

            chunks_processed += 1

            # Log progress every 10 chunks
            if chunks_processed % 10 == 0:
                logger.info(
                    f"Progress: {chunks_processed} chunks, "
                    f"{total_bronze:,} processed, "
                    f"{total_silver:,} loaded"
                )

    logger.info(
        f"Completed: {chunks_processed} chunks, "
        f"{total_bronze:,} bronze records, "
        f"{total_silver:,} silver records"
    )

    return {
        "bronze_records": total_bronze,
        "silver_records": total_silver,
        "skipped": total_bronze - total_silver,
        "chunks_processed": chunks_processed,
    }


@task(name="validate_md_transformation")
def validate_md_transformation_task(
    entity_type: str,
    bronze_count: int,
    silver_count: int,
    skipped_count: int,
) -> dict[str, Any]:
    """
    Validate transformation results.

    Args:
        entity_type: Type of entity (committee, candidate, contribution)
        bronze_count: Number of Bronze records processed
        silver_count: Number of Silver records created
        skipped_count: Number of records skipped

    Returns:
        Validation results with warnings/errors
    """
    warnings = []
    errors = []

    # Check for data loss
    if silver_count < bronze_count - skipped_count:
        warnings.append(
            f"Data loss detected: {bronze_count} bronze → {silver_count} silver "
            f"({bronze_count - silver_count - skipped_count} records lost)"
        )

    # Check for empty results
    if bronze_count > 0 and silver_count == 0:
        errors.append(f"No Silver records created from {bronze_count} Bronze records")

    # Check skip rate
    skip_rate = (skipped_count / bronze_count * 100) if bronze_count > 0 else 0
    if skip_rate > 50:
        warnings.append(f"High skip rate: {skip_rate:.1f}% ({skipped_count}/{bronze_count})")

    status = "error" if errors else "warning" if warnings else "success"

    return {
        "entity_type": entity_type,
        "status": status,
        "bronze_count": bronze_count,
        "silver_count": silver_count,
        "skipped_count": skipped_count,
        "skip_rate_pct": round(skip_rate, 2),
        "warnings": warnings,
        "errors": errors,
    }


@flow(
    name="maryland_silver_transformation",
    description="Transform Maryland Bronze data to Silver layer",
)
def maryland_silver_transformation_flow(
    election_year: int | None = None,
    include_committees: bool = True,
    include_candidates: bool = True,
    include_contributions: bool = True,
) -> dict[str, Any]:
    """
    Transform Maryland Bronze layer data to Silver layer.

    Transforms committees, candidates, and contributions in sequence.
    Contributions are enriched with committee data.

    Args:
        election_year: Optional election year filter for candidates
        include_committees: Whether to transform committees
        include_candidates: Whether to transform candidates
        include_contributions: Whether to transform contributions

    Returns:
        Dict with comprehensive transformation results and validation
    """
    from prefect import get_run_logger

    logger = get_run_logger()

    results: dict[str, Any] = {
        "filters": {
            "election_year": election_year,
        },
        "transformations": {},
        "validations": {},
    }

    # Step 1: Transform Committees (must come first for enrichment)
    if include_committees:
        logger.info("Step 1: Transforming committees...")
        committee_result = transform_md_committees_task()
        results["transformations"]["committees"] = committee_result

        committee_validation = validate_md_transformation_task(
            entity_type="committee",
            bronze_count=committee_result["bronze_records"],
            silver_count=committee_result["silver_records"],
            skipped_count=committee_result["skipped"],
        )
        results["validations"]["committees"] = committee_validation
    else:
        logger.info("Step 1: Skipping committees")

    # Step 2: Transform Candidates
    if include_candidates:
        logger.info("Step 2: Transforming candidates...")
        candidate_result = transform_md_candidates_task(election_year=election_year)
        results["transformations"]["candidates"] = candidate_result

        candidate_validation = validate_md_transformation_task(
            entity_type="candidate",
            bronze_count=candidate_result["bronze_records"],
            silver_count=candidate_result["silver_records"],
            skipped_count=candidate_result["skipped"],
        )
        results["validations"]["candidates"] = candidate_validation
    else:
        logger.info("Step 2: Skipping candidates")

    # Step 3: Transform Contributions
    if include_contributions:
        logger.info("Step 3: Transforming contributions...")
        contribution_result = transform_md_contributions_task()
        results["transformations"]["contributions"] = contribution_result

        contribution_validation = validate_md_transformation_task(
            entity_type="contribution",
            bronze_count=contribution_result["bronze_records"],
            silver_count=contribution_result["silver_records"],
            skipped_count=contribution_result["skipped"],
        )
        results["validations"]["contributions"] = contribution_validation
    else:
        logger.info("Step 3: Skipping contributions")

    # Summary
    total_bronze = sum(r.get("bronze_records", 0) for r in results["transformations"].values())
    total_silver = sum(r.get("silver_records", 0) for r in results["transformations"].values())
    total_skipped = sum(r.get("skipped", 0) for r in results["transformations"].values())

    results["summary"] = {
        "total_bronze_records": total_bronze,
        "total_silver_records": total_silver,
        "total_skipped": total_skipped,
        "validation_status": {k: v["status"] for k, v in results.get("validations", {}).items()},
    }

    logger.info("\n" + "=" * 70)
    logger.info("Maryland Silver Transformation Complete!")
    logger.info(f"  Total Bronze: {total_bronze:,}")
    logger.info(f"  Total Silver: {total_silver:,}")
    logger.info(f"  Total Skipped: {total_skipped:,}")
    logger.info("=" * 70)

    return results


# ============================================================================
# CONVENIENCE FLOWS
# ============================================================================


@flow(
    name="maryland_silver_incremental",
    description="Incremental Maryland Bronze to Silver transformation",
)
def maryland_silver_incremental_flow() -> dict[str, Any]:
    """
    Incremental transformation for Maryland data.

    Only transforms Bronze records not yet in Silver.

    Returns:
        Summary statistics
    """
    return maryland_silver_transformation_flow(
        include_committees=True,
        include_candidates=True,
        include_contributions=True,
    )


@flow(
    name="maryland_silver_contributions_only",
    description="Transform only Maryland contributions to Silver",
)
def maryland_silver_contributions_only_flow() -> dict[str, Any]:
    """
    Transform only contributions (for daily incremental runs).

    Assumes committees are already in Silver for enrichment.

    Returns:
        Summary statistics
    """
    return maryland_silver_transformation_flow(
        include_committees=False,
        include_candidates=False,
        include_contributions=True,
    )
