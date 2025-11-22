"""
Silver Transformation Flow

Transforms Bronze layer data into Silver layer (cleaned and enriched).
- Committees: Standardize and clean
- Candidates: Standardize and clean
- Contributions: Clean and enrich with committee/candidate JOINs
"""

import math
from datetime import date
from typing import Any

import pandas as pd
from fund_lens_models.bronze import BronzeFECCandidate, BronzeFECCommittee, BronzeFECScheduleA
from fund_lens_models.silver import (
    SilverFECCandidate,
    SilverFECCommittee,
    SilverFECContribution,
)
from prefect import flow, task
from sqlalchemy import select

from fund_lens_etl.database import get_session
from fund_lens_etl.transformers.bronze_to_silver import BronzeToSilverFECTransformer
from fund_lens_etl.transformers.bronze_to_silver_entities import (
    BronzeToSilverCandidateTransformer,
    BronzeToSilverCommitteeTransformer,
)


def clean_nan_values(data_dict: dict[str, Any]) -> dict[str, Any]:
    """
    Replace NaN values with None for database insertion.

    Pandas uses float NaN to represent missing data, but PostgreSQL
    can't handle Python's NaN properly, causing type errors.

    Args:
        data_dict: Dictionary potentially containing NaN values

    Returns:
        Dictionary with NaN values replaced by None
    """
    return {
        k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in data_dict.items()
    }


# Retry configuration for transformation tasks
SILVER_RETRY_CONFIG = {
    "retries": 3,
    "retry_delay_seconds": 10,
}
SILVER_TASK_TIMEOUT = 1800  # 30 minutes


@task(
    name="transform_committees",
    retries=SILVER_RETRY_CONFIG["retries"],
    retry_delay_seconds=SILVER_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=SILVER_TASK_TIMEOUT,
)
def transform_committees_task(
    state: str | None = None,
    cycle: int | None = None,
) -> dict[str, Any]:
    """
    Transform Bronze committees to Silver.

    Args:
        state: Optional state filter (e.g., 'MD')
        cycle: Optional election cycle filter (e.g., 2026)

    Returns:
        Dict with transformation stats
    """
    with get_session() as session:
        # Fetch all Bronze committees
        stmt = select(BronzeFECCommittee)
        bronze_committees = session.execute(stmt).scalars().all()

        # Filter in Python
        if state:
            bronze_committees = [c for c in bronze_committees if c.state == state]

        if cycle:
            bronze_committees = [
                c for c in bronze_committees if c.raw_json and cycle in c.raw_json.get("cycles", [])
            ]

        if not bronze_committees:
            return {
                "bronze_records": 0,
                "silver_records": 0,
                "skipped": 0,
            }

        # Get all column names from Bronze model (excluding metadata)
        exclude_cols = {
            "created_at",
            "updated_at",
            "ingestion_timestamp",
            "source_system",
            "metadata",
            "registry",
        }
        bronze_cols = [
            col.name
            for col in BronzeFECCommittee.__table__.columns.values()
            if col.name not in exclude_cols
        ]

        # Convert to DataFrame
        bronze_df = pd.DataFrame(
            [{col: getattr(c, col) for col in bronze_cols} for c in bronze_committees]
        )

        # Transform
        transformer = BronzeToSilverCommitteeTransformer()
        silver_df = transformer.transform(bronze_df, election_cycle=cycle)

        # Get valid Silver model columns
        valid_columns = {col.name for col in SilverFECCommittee.__table__.columns.values()}
        valid_columns.discard("id")
        valid_columns.discard("created_at")
        valid_columns.discard("updated_at")

        # Load to Silver with UPSERT logic
        records_loaded = 0
        for _, row in silver_df.iterrows():
            row_dict = row.to_dict()

            # Filter to only valid columns
            row_dict = {k: v for k, v in row_dict.items() if k in valid_columns}

            # Clean NaN values (convert to None for database)
            row_dict = clean_nan_values(row_dict)

            # Check if record exists
            existing = session.execute(
                select(SilverFECCommittee).where(
                    SilverFECCommittee.source_committee_id == row_dict["source_committee_id"]
                )
            ).scalar_one_or_none()

            if existing:
                # Update existing record
                for key, value in row_dict.items():
                    setattr(existing, key, value)
            else:
                # Insert new record
                silver_committee = SilverFECCommittee(**row_dict)
                session.add(silver_committee)

            records_loaded += 1

        session.commit()

        return {
            "bronze_records": len(bronze_committees),
            "silver_records": records_loaded,
            "skipped": len(bronze_committees) - records_loaded,
        }


@task(
    name="transform_candidates",
    retries=SILVER_RETRY_CONFIG["retries"],
    retry_delay_seconds=SILVER_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=SILVER_TASK_TIMEOUT,
)
def transform_candidates_task(
    state: str | None = None,
    cycle: int | None = None,
) -> dict[str, Any]:
    """
    Transform Bronze candidates to Silver.

    Args:
        state: Optional state filter (e.g., 'MD')
        cycle: Optional election cycle filter (e.g., 2026)

    Returns:
        Dict with transformation stats
    """
    with get_session() as session:
        # Fetch all Bronze candidates
        stmt = select(BronzeFECCandidate)
        bronze_candidates = session.execute(stmt).scalars().all()

        # Filter in Python
        if state:
            bronze_candidates = [c for c in bronze_candidates if c.state == state]

        if cycle:
            bronze_candidates = [
                c
                for c in bronze_candidates
                if c.raw_json
                and (
                    cycle in c.raw_json.get("election_years", [])
                    or any(abs(year - cycle) <= 2 for year in c.raw_json.get("election_years", []))
                )
            ]

        if not bronze_candidates:
            return {
                "bronze_records": 0,
                "silver_records": 0,
                "skipped": 0,
            }

        # Get all column names from Bronze model (excluding metadata)
        exclude_cols = {
            "created_at",
            "updated_at",
            "ingestion_timestamp",
            "source_system",
            "metadata",
            "registry",
        }
        bronze_cols = [
            col.name
            for col in BronzeFECCandidate.__table__.columns.values()
            if col.name not in exclude_cols
        ]

        # Convert to DataFrame
        bronze_df = pd.DataFrame(
            [{col: getattr(c, col) for col in bronze_cols} for c in bronze_candidates]
        )

        # Transform
        transformer = BronzeToSilverCandidateTransformer()
        silver_df = transformer.transform(bronze_df, election_cycle=cycle)

        # Get valid Silver model columns
        valid_columns = {col.name for col in SilverFECCandidate.__table__.columns.values()}
        valid_columns.discard("id")
        valid_columns.discard("created_at")
        valid_columns.discard("updated_at")

        # Load to Silver with UPSERT logic
        records_loaded = 0
        for _, row in silver_df.iterrows():
            row_dict = row.to_dict()

            # Filter to only valid columns
            row_dict = {k: v for k, v in row_dict.items() if k in valid_columns}

            # Clean NaN values (convert to None for database)
            row_dict = clean_nan_values(row_dict)

            # Check if record exists
            existing = session.execute(
                select(SilverFECCandidate).where(
                    SilverFECCandidate.source_candidate_id == row_dict["source_candidate_id"]
                )
            ).scalar_one_or_none()

            if existing:
                # Update existing record
                for key, value in row_dict.items():
                    setattr(existing, key, value)
            else:
                # Insert new record
                silver_candidate = SilverFECCandidate(**row_dict)
                session.add(silver_candidate)

            records_loaded += 1

        session.commit()

        return {
            "bronze_records": len(bronze_candidates),
            "silver_records": records_loaded,
            "skipped": len(bronze_candidates) - records_loaded,
        }


@task(
    name="transform_contributions",
    retries=SILVER_RETRY_CONFIG["retries"],
    retry_delay_seconds=SILVER_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=SILVER_TASK_TIMEOUT * 4,  # 2 hours for large datasets
)
def transform_contributions_task(
    state: str | None = None,
    cycle: int | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    chunksize: int = 10_000,
) -> dict[str, Any]:
    """
    Transform Bronze contributions to Silver with committee/candidate enrichment.

    OPTIMIZED: Processes in chunks to avoid loading millions of records into memory.

    Args:
        state: Unused - kept for API compatibility. Bronze ingestion already filters
               to committees from target state. Silver transforms all bronze records.
        cycle: Optional election cycle filter (e.g., 2026)
        start_date: Optional start date filter
        end_date: Optional end date filter
        chunksize: Number of records to process per chunk (default: 10,000)

    Returns:
        Dict with transformation stats
    """
    from prefect import get_run_logger
    from sqlalchemy import func

    logger = get_run_logger()

    with get_session() as session:
        # Build base query with filters
        stmt = select(BronzeFECScheduleA)

        if cycle:
            stmt = stmt.where(BronzeFECScheduleA.two_year_transaction_period == cycle)
        if start_date:
            stmt = stmt.where(BronzeFECScheduleA.contribution_receipt_date >= start_date)
        if end_date:
            stmt = stmt.where(BronzeFECScheduleA.contribution_receipt_date <= end_date)

        # Get total count for progress tracking
        count_stmt = select(func.count()).select_from(BronzeFECScheduleA)
        if cycle:
            count_stmt = count_stmt.where(BronzeFECScheduleA.two_year_transaction_period == cycle)
        if start_date:
            count_stmt = count_stmt.where(
                BronzeFECScheduleA.contribution_receipt_date >= start_date
            )
        if end_date:
            count_stmt = count_stmt.where(BronzeFECScheduleA.contribution_receipt_date <= end_date)

        total_count = session.execute(count_stmt).scalar()
        logger.info(f"Processing {total_count:,} bronze contributions in chunks of {chunksize:,}")

        if total_count == 0:
            return {
                "bronze_records": 0,
                "silver_records": 0,
                "skipped": 0,
                "chunks_processed": 0,
            }

        # Get all column names from Bronze model (excluding metadata)
        exclude_cols = {
            "created_at",
            "updated_at",
            "ingestion_timestamp",
            "source_system",
            "metadata",
            "registry",
        }
        bronze_cols = [
            col.name
            for col in BronzeFECScheduleA.__table__.columns.values()
            if col.name not in exclude_cols
        ]

        # Get valid Silver model columns
        valid_columns = {col.name for col in SilverFECContribution.__table__.columns.values()}
        valid_columns.discard("id")
        valid_columns.discard("created_at")
        valid_columns.discard("updated_at")

        # Process in chunks
        total_bronze = 0
        total_silver = 0
        chunks_processed = 0
        offset = 0

        while offset < total_count:
            # Fetch chunk
            chunk_stmt = stmt.limit(chunksize).offset(offset)
            bronze_chunk = session.execute(chunk_stmt).scalars().all()

            if not bronze_chunk:
                break

            chunk_size = len(bronze_chunk)
            total_bronze += chunk_size

            # Convert to DataFrame
            bronze_df = pd.DataFrame(
                [{col: getattr(c, col) for col in bronze_cols} for c in bronze_chunk]
            )

            # Transform with session for enrichment (transformer handles JOINs)
            transformer = BronzeToSilverFECTransformer(session=session)
            silver_df = transformer.transform(bronze_df)

            # Check which source_sub_ids already exist in Silver
            chunk_sub_ids = silver_df["sub_id"].tolist()
            existing_sub_ids_query = select(SilverFECContribution.source_sub_id).where(
                SilverFECContribution.source_sub_id.in_(chunk_sub_ids)
            )
            existing_sub_ids = set(session.execute(existing_sub_ids_query).scalars().all())

            # Separate new and existing records
            new_records_df = silver_df[~silver_df["sub_id"].isin(existing_sub_ids)]
            update_records_df = silver_df[silver_df["sub_id"].isin(existing_sub_ids)]

            # Bulk insert new records
            if len(new_records_df) > 0:
                new_records = []
                for _, row in new_records_df.iterrows():
                    row_dict = row.to_dict()
                    if "sub_id" in row_dict:
                        row_dict["source_sub_id"] = row_dict.pop("sub_id")
                    row_dict = {k: v for k, v in row_dict.items() if k in valid_columns}
                    row_dict = clean_nan_values(row_dict)
                    new_records.append(SilverFECContribution(**row_dict))

                session.bulk_save_objects(new_records)
                total_silver += len(new_records)

            # Update existing records (slower, but fewer of these)
            for _, row in update_records_df.iterrows():
                row_dict = row.to_dict()
                source_sub_id = row_dict.pop("sub_id") if "sub_id" in row_dict else None
                row_dict = {k: v for k, v in row_dict.items() if k in valid_columns}
                row_dict = clean_nan_values(row_dict)

                existing = session.execute(
                    select(SilverFECContribution).where(
                        SilverFECContribution.source_sub_id == source_sub_id
                    )
                ).scalar_one_or_none()

                if existing:
                    for key, value in row_dict.items():
                        if key != "source_sub_id":
                            setattr(existing, key, value)
                    total_silver += 1

            session.commit()
            chunks_processed += 1
            offset += chunksize

            # Log progress every 10 chunks
            if chunks_processed % 10 == 0:
                logger.info(
                    f"Progress: {chunks_processed} chunks, "
                    f"{total_bronze:,} processed, "
                    f"{total_silver:,} loaded/updated"
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


@task(name="validate_transformation")
def validate_transformation_task(
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


@flow(name="silver_transformation_flow")
def silver_transformation_flow(
    state: str | None = None,
    cycle: int | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, Any]:
    """
    Transform Bronze layer data to Silver layer.

    Transforms committees, candidates, and contributions in sequence.
    Contributions depend on committees and candidates for enrichment.

    Args:
        state: Optional state filter (e.g., 'MD'). Defaults to all states.
        cycle: Optional election cycle filter (e.g., 2026). Defaults to all cycles.
        start_date: Optional start date for contributions. Defaults to all dates.
        end_date: Optional end date for contributions. Defaults to all dates.

    Returns:
        Dict with comprehensive transformation results and validation
    """
    results: dict[str, Any] = {
        "filters": {
            "state": state,
            "cycle": cycle,
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
        },
        "transformations": {},
        "validations": {},
    }

    # Step 1: Transform Committees
    committee_result = transform_committees_task(state=state, cycle=cycle)
    results["transformations"]["committees"] = committee_result

    committee_validation = validate_transformation_task(
        entity_type="committee",
        bronze_count=committee_result["bronze_records"],
        silver_count=committee_result["silver_records"],
        skipped_count=committee_result["skipped"],
    )
    results["validations"]["committees"] = committee_validation

    # Step 2: Transform Candidates
    candidate_result = transform_candidates_task(state=state, cycle=cycle)
    results["transformations"]["candidates"] = candidate_result

    candidate_validation = validate_transformation_task(
        entity_type="candidate",
        bronze_count=candidate_result["bronze_records"],
        silver_count=candidate_result["silver_records"],
        skipped_count=candidate_result["skipped"],
    )
    results["validations"]["candidates"] = candidate_validation

    # Step 3: Transform Contributions (depends on committees and candidates)
    contribution_result = transform_contributions_task(
        state=state,
        cycle=cycle,
        start_date=start_date,
        end_date=end_date,
    )
    results["transformations"]["contributions"] = contribution_result

    contribution_validation = validate_transformation_task(
        entity_type="contribution",
        bronze_count=contribution_result["bronze_records"],
        silver_count=contribution_result["silver_records"],
        skipped_count=contribution_result["skipped"],
    )
    results["validations"]["contributions"] = contribution_validation

    # Summary
    results["summary"] = {
        "total_bronze_records": sum(
            r["bronze_records"] for r in results["transformations"].values()
        ),
        "total_silver_records": sum(
            r["silver_records"] for r in results["transformations"].values()
        ),
        "total_skipped": sum(r["skipped"] for r in results["transformations"].values()),
        "validation_status": {
            "committees": committee_validation["status"],
            "candidates": candidate_validation["status"],
            "contributions": contribution_validation["status"],
        },
    }

    return results
