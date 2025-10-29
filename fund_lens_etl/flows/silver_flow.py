"""Prefect flow for transforming bronze data to silver layer."""

import pandas as pd
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NONE as NO_CACHE
from sqlalchemy import select
from sqlalchemy.orm import Session

from fund_lens_etl.config import validate_election_cycle
from fund_lens_etl.database import get_db_session
from fund_lens_etl.models.bronze import BronzeFECScheduleA
from fund_lens_etl.models.silver import SilverFECContribution
from fund_lens_etl.transformers import BronzeToSilverFECTransformer


def safe_extract_value(value):
    """Safely extract Python value from pandas/numpy types."""
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


@task(cache_policy=NO_CACHE)
def get_unprocessed_bronze_records(
    session: Session,
    committee_id: str | None = None,
    election_cycle: int | None = None,
    batch_size: int = 1000,
) -> list[BronzeFECScheduleA]:
    """
    Get bronze records that haven't been processed to silver yet.

    Args:
        session: Database session
        committee_id: Optional committee ID filter
        election_cycle: Optional election cycle filter
        batch_size: Number of records to fetch

    Returns:
        List of bronze records
    """
    logger = get_run_logger()

    # Build query with LEFT JOIN to find records not in silver
    stmt = (
        select(BronzeFECScheduleA)
        .outerjoin(
            SilverFECContribution, BronzeFECScheduleA.sub_id == SilverFECContribution.source_sub_id
        )
        .where(SilverFECContribution.id.is_(None))
    )

    # Filter by committee if specified
    if committee_id:
        stmt = stmt.where(BronzeFECScheduleA.committee_id == committee_id)

    # Filter by cycle if specified
    if election_cycle:
        stmt = stmt.where(BronzeFECScheduleA.two_year_transaction_period == election_cycle)

    # Order by date for consistent processing
    stmt = stmt.order_by(BronzeFECScheduleA.contribution_receipt_date)

    # Limit batch size
    stmt = stmt.limit(batch_size)

    result = session.execute(stmt)
    records = list(result.scalars().all())

    logger.info(f"Found {len(records)} unprocessed bronze records")

    return records


@task(cache_policy=NO_CACHE)
def transform_and_load_to_silver(
    session: Session,
    bronze_records: list[BronzeFECScheduleA],
) -> int:
    """
    Transform bronze records and load to silver layer.

    Args:
        session: Database session
        bronze_records: List of bronze records to transform

    Returns:
        Number of records loaded to silver
    """
    logger = get_run_logger()

    if not bronze_records:
        logger.info("No bronze records to process")
        return 0

    # Convert to DataFrame
    bronze_data = []
    for record in bronze_records:
        # Convert ORM object to dict
        record_dict = {
            "sub_id": record.sub_id,
            "transaction_id": record.transaction_id,
            "file_number": record.file_number,
            "contribution_receipt_date": record.contribution_receipt_date,
            "contribution_receipt_amount": record.contribution_receipt_amount,
            "contributor_aggregate_ytd": record.contributor_aggregate_ytd,
            "contributor_name": record.contributor_name,
            "contributor_first_name": record.contributor_first_name,
            "contributor_last_name": record.contributor_last_name,
            "contributor_city": record.contributor_city,
            "contributor_state": record.contributor_state,
            "contributor_zip": record.contributor_zip,
            "contributor_employer": record.contributor_employer,
            "contributor_occupation": record.contributor_occupation,
            "entity_type": record.entity_type,
            "committee_id": record.committee_id,
            "committee": {"name": None, "committee_type": None},
            "recipient_committee_designation": record.recipient_committee_designation,
            "receipt_type": record.receipt_type,
            "election_type": record.election_type,
            "memo_text": record.memo_text,
            "two_year_transaction_period": record.two_year_transaction_period,
            "report_year": record.report_year,
        }
        bronze_data.append(record_dict)

    bronze_df = pd.DataFrame(bronze_data)

    # Transform to silver
    transformer = BronzeToSilverFECTransformer()
    silver_df = transformer.transform(bronze_df)

    if silver_df.empty:
        logger.warning("Transformation resulted in empty DataFrame")
        return 0

    # Load to silver layer
    loaded_count = 0
    for _, row in silver_df.iterrows():
        source_sub_id = str(row["sub_id"])

        # Check if already exists
        stmt = select(SilverFECContribution).where(
            SilverFECContribution.source_sub_id == source_sub_id
        )
        exists = session.execute(stmt).scalar_one_or_none() is not None

        if exists:
            continue

        silver_record = SilverFECContribution(
            source_sub_id=source_sub_id,
            transaction_id=str(row["transaction_id"]),
            file_number=int(safe_extract_value(row["file_number"]))
            if pd.notna(row["file_number"])
            else None,
            contribution_date=row["contribution_date"].date()
            if hasattr(row["contribution_date"], "date")
            else row["contribution_date"],
            contribution_amount=float(safe_extract_value(row["contribution_amount"])),
            contributor_aggregate_ytd=float(safe_extract_value(row["contributor_aggregate_ytd"]))
            if pd.notna(row["contributor_aggregate_ytd"])
            else None,
            contributor_name=str(row["contributor_name"]),
            contributor_first_name=str(row["contributor_first_name"])
            if pd.notna(row["contributor_first_name"])
            else None,
            contributor_last_name=str(row["contributor_last_name"])
            if pd.notna(row["contributor_last_name"])
            else None,
            contributor_city=str(row["contributor_city"])
            if pd.notna(row["contributor_city"])
            else None,
            contributor_state=str(row["contributor_state"])
            if pd.notna(row["contributor_state"])
            else None,
            contributor_zip=str(row["contributor_zip"])
            if pd.notna(row["contributor_zip"])
            else None,
            contributor_employer=str(row["contributor_employer"]),
            contributor_occupation=str(row["contributor_occupation"]),
            entity_type=str(row["entity_type"]),
            committee_id=str(row["committee_id"]),
            committee_name=str(row["committee_name"]) if pd.notna(row["committee_name"]) else None,
            committee_type=str(row["committee_type"]) if pd.notna(row["committee_type"]) else None,
            committee_designation=str(row["committee_designation"])
            if pd.notna(row["committee_designation"])
            else None,
            receipt_type=str(row["receipt_type"]) if pd.notna(row["receipt_type"]) else None,
            election_type=str(row["election_type"]) if pd.notna(row["election_type"]) else None,
            memo_text=str(row["memo_text"]) if pd.notna(row["memo_text"]) else None,
            election_cycle=int(safe_extract_value(row["election_cycle"])),
            report_year=int(safe_extract_value(row["report_year"]))
            if pd.notna(row["report_year"])
            else None,
        )

        session.add(silver_record)
        loaded_count += 1

    session.commit()
    logger.info(f"Loaded {loaded_count} records to silver layer")

    return loaded_count


@flow(name="Bronze to Silver", log_prints=True)
def bronze_to_silver_flow(
    election_cycle: int | None = None,
    committee_id: str | None = None,
    batch_size: int = 1000,
    max_batches: int | None = None,
) -> dict[str, int]:
    """
    Transform bronze FEC data to silver layer in batches.

    Args:
        election_cycle: Optional election cycle filter
        committee_id: Optional committee ID filter
        batch_size: Number of records per batch
        max_batches: Maximum number of batches to process (None = all)

    Returns:
        Dictionary with transformation statistics
    """
    logger = get_run_logger()

    if election_cycle:
        election_cycle = validate_election_cycle(election_cycle)

    logger.info("Starting Bronze to Silver flow")
    if committee_id:
        logger.info(f"Processing committee: {committee_id}")
    if election_cycle:
        logger.info(f"Processing cycle: {election_cycle}")

    total_processed = 0
    batch_count = 0

    with get_db_session() as session:
        while True:
            # Check if we've hit max batches
            if max_batches and batch_count >= max_batches:
                logger.info(f"Reached max_batches limit ({max_batches})")
                break

            # Get next batch of unprocessed records
            bronze_records = get_unprocessed_bronze_records(
                session=session,
                committee_id=committee_id,
                election_cycle=election_cycle,
                batch_size=batch_size,
            )

            if not bronze_records:
                logger.info("No more bronze records to process")
                break

            # Transform and load
            loaded = transform_and_load_to_silver(session, bronze_records)
            total_processed += loaded
            batch_count += 1

            logger.info(
                f"Batch {batch_count}: {loaded} records processed " f"(total: {total_processed})"
            )

    logger.info(
        f"Flow complete: {total_processed} total records processed in {batch_count} batches"
    )

    return {
        "batches_processed": batch_count,
        "total_records_processed": total_processed,
    }
