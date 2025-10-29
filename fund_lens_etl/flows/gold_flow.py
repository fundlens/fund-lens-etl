"""Prefect flow for transforming silver data to gold layer."""

import pandas as pd
from prefect import flow, get_run_logger, task
from sqlalchemy import select
from sqlalchemy.orm import Session

from fund_lens_etl.config import validate_election_cycle
from fund_lens_etl.database import get_db_session
from fund_lens_etl.models.gold import GoldContribution
from fund_lens_etl.models.silver import SilverFECContribution
from fund_lens_etl.transformers import SilverToGoldFECTransformer


@task
def get_unprocessed_silver_records(
    session: Session,
    committee_id: str | None = None,
    election_cycle: int | None = None,
    batch_size: int = 1000,
) -> list[SilverFECContribution]:
    """
    Get silver records that haven't been processed to gold yet.

    Args:
        session: Database session
        committee_id: Optional committee ID filter
        election_cycle: Optional election cycle filter
        batch_size: Number of records to fetch

    Returns:
        List of silver records
    """
    logger = get_run_logger()

    # Build query with LEFT JOIN to find records not in gold
    stmt = (
        select(SilverFECContribution)
        .outerjoin(
            GoldContribution,
            (GoldContribution.source_system == "FEC")
            & (GoldContribution.source_transaction_id == SilverFECContribution.source_sub_id),
        )
        .where(GoldContribution.id.is_(None))
    )

    # Filter by committee if specified
    if committee_id:
        stmt = stmt.where(SilverFECContribution.committee_id == committee_id)

    # Filter by cycle if specified
    if election_cycle:
        stmt = stmt.where(SilverFECContribution.election_cycle == election_cycle)

    # Order by date for consistent processing
    stmt = stmt.order_by(SilverFECContribution.contribution_date)

    # Limit batch size
    stmt = stmt.limit(batch_size)

    result = session.execute(stmt)
    records = list(result.scalars().all())

    logger.info(f"Found {len(records)} unprocessed silver records")

    return records


@task
def transform_and_load_to_gold(
    session: Session,
    silver_records: list[SilverFECContribution],
) -> int:
    """
    Transform silver records and load to gold layer.

    Args:
        session: Database session
        silver_records: List of silver records to transform

    Returns:
        Number of records loaded to gold
    """
    logger = get_run_logger()

    if not silver_records:
        logger.info("No silver records to process")
        return 0

    # Convert to DataFrame
    silver_data = []
    for record in silver_records:
        # Convert ORM object to dict
        record_dict = {
            "bronze_sub_id": record.source_sub_id,
            "transaction_id": record.transaction_id,
            "contribution_date": record.contribution_date,
            "contribution_amount": record.contribution_amount,
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
            "committee_name": record.committee_name,
            "committee_type": record.committee_type,
            "committee_designation": record.committee_designation,
            "receipt_type": record.receipt_type,
            "election_type": record.election_type,
            "memo_text": record.memo_text,
            "election_cycle": record.election_cycle,
            "report_year": record.report_year,
        }
        silver_data.append(record_dict)

    silver_df = pd.DataFrame(silver_data)

    # Transform to gold (with entity resolution)
    transformer = SilverToGoldFECTransformer(session=session)
    gold_df = transformer.transform(silver_df)

    if gold_df.empty:
        logger.warning("Transformation resulted in empty DataFrame")
        return 0

    # Load to gold layer
    loaded_count = 0
    for _, row in gold_df.iterrows():
        source_transaction_id = str(row["source_transaction_id"])

        # Check if already exists (double-check despite transformer logic)
        stmt = select(GoldContribution).where(
            GoldContribution.source_system == "FEC",
            GoldContribution.source_transaction_id == source_transaction_id,
        )
        exists = session.execute(stmt).scalar_one_or_none() is not None

        if exists:
            continue

        gold_record = GoldContribution(
            source_system=str(row["source_system"]),
            source_transaction_id=source_transaction_id,
            contribution_date=row["contribution_date"].date()
            if hasattr(row["contribution_date"], "date")
            else row["contribution_date"],
            amount=float(row["amount"]),  # type: ignore[arg-type]
            contributor_id=int(row["contributor_id"]),  # type: ignore[arg-type]
            recipient_committee_id=int(row["recipient_committee_id"]),  # type: ignore[arg-type]
            recipient_candidate_id=int(row["recipient_candidate_id"])  # type: ignore[arg-type]
            if pd.notna(row["recipient_candidate_id"])
            else None,
            contribution_type=str(row["contribution_type"]),
            election_type=str(row["election_type"]) if pd.notna(row["election_type"]) else None,
            election_year=int(row["election_year"]),  # type: ignore[arg-type]
            election_cycle=int(row["election_cycle"]),  # type: ignore[arg-type]
            memo_text=str(row["memo_text"]) if pd.notna(row["memo_text"]) else None,
        )

        session.add(gold_record)
        loaded_count += 1

    session.commit()
    logger.info(f"Loaded {loaded_count} records to gold layer")

    return loaded_count


@flow(name="Silver to Gold", log_prints=True)
def silver_to_gold_flow(
    election_cycle: int | None = None,
    committee_id: str | None = None,
    batch_size: int = 1000,
    max_batches: int | None = None,
) -> dict[str, int]:
    """
    Transform silver FEC data to gold layer in batches with entity resolution.

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

    logger.info("Starting Silver to Gold flow")
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
            silver_records = get_unprocessed_silver_records(
                session=session,
                committee_id=committee_id,
                election_cycle=election_cycle,
                batch_size=batch_size,
            )

            if not silver_records:
                logger.info("No more silver records to process")
                break

            # Transform and load
            loaded = transform_and_load_to_gold(session, silver_records)
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
