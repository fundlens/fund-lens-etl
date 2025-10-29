"""Prefect flow for extracting FEC data and loading to bronze layer."""

from datetime import UTC, date, datetime

from prefect import flow, get_run_logger, task
from prefect.cache_policies import NONE as NO_CACHE
from sqlalchemy import select
from sqlalchemy.orm import Session

from fund_lens_etl.config import USState, validate_election_cycle
from fund_lens_etl.database import get_db_session
from fund_lens_etl.extractors import FECAPIExtractor
from fund_lens_etl.models.bronze import BronzeFECExtractionState, BronzeFECScheduleA


@task(cache_policy=NO_CACHE)
def get_extraction_state(
    session: Session, committee_id: str, election_cycle: int
) -> BronzeFECExtractionState | None:
    """
    Get the last extraction state for a committee.

    Args:
        session: Database session
        committee_id: FEC committee ID
        election_cycle: Election cycle year

    Returns:
        Extraction state or None if first run
    """
    logger = get_run_logger()

    stmt = select(BronzeFECExtractionState).where(
        BronzeFECExtractionState.committee_id == committee_id,
        BronzeFECExtractionState.election_cycle == election_cycle,
    )

    state = session.execute(stmt).scalar_one_or_none()

    if state:
        logger.info(
            f"Found existing state for {committee_id}: "
            f"last_date={state.last_contribution_date}, "
            f"total_extracted={state.total_contributions_extracted}"
        )
    else:
        logger.info(f"No existing state for {committee_id} - this is an initial load")

    return state


@task(cache_policy=NO_CACHE)
def load_page_to_bronze(
    session: Session,
    page_df,
    committee_id: str,
    election_cycle: int,
) -> int:
    """
    Load a page of contributions to bronze layer.

    Args:
        session: Database session
        page_df: DataFrame with contribution data
        committee_id: FEC committee ID
        election_cycle: Election cycle year

    Returns:
        Number of records loaded (excluding duplicates)
    """
    logger = get_run_logger()

    if page_df.empty:
        logger.warning("Empty page received, skipping")
        return 0

    logger.info(f"Loading page for committee {committee_id}, cycle {election_cycle}")

    loaded_count = 0
    duplicate_count = 0

    for _, row in page_df.iterrows():
        sub_id = row.get("sub_id")

        # Check if already exists
        stmt = select(BronzeFECScheduleA).where(BronzeFECScheduleA.sub_id == sub_id)
        exists = session.execute(stmt).scalar_one_or_none() is not None

        if exists:
            duplicate_count += 1
            continue

        # Create bronze record
        bronze_record = BronzeFECScheduleA(
            sub_id=sub_id,
            transaction_id=row.get("transaction_id"),
            file_number=row.get("file_number"),
            amendment_indicator=row.get("amendment_indicator"),
            contribution_receipt_date=row.get("contribution_receipt_date"),
            contribution_receipt_amount=row.get("contribution_receipt_amount"),
            contributor_aggregate_ytd=row.get("contributor_aggregate_ytd"),
            contributor_name=row.get("contributor_name"),
            contributor_first_name=row.get("contributor_first_name"),
            contributor_last_name=row.get("contributor_last_name"),
            contributor_middle_name=row.get("contributor_middle_name"),
            contributor_city=row.get("contributor_city"),
            contributor_state=row.get("contributor_state"),
            contributor_zip=row.get("contributor_zip"),
            contributor_employer=row.get("contributor_employer"),
            contributor_occupation=row.get("contributor_occupation"),
            entity_type=row.get("entity_type"),
            committee_id=row.get("committee_id"),
            recipient_committee_designation=row.get("recipient_committee_designation"),
            recipient_committee_type=row.get("recipient_committee_type"),
            recipient_committee_org_type=row.get("recipient_committee_org_type"),
            receipt_type=row.get("receipt_type"),
            election_type=row.get("election_type"),
            memo_text=row.get("memo_text"),
            memo_code=row.get("memo_code"),
            two_year_transaction_period=row.get("two_year_transaction_period"),
            report_year=row.get("report_year"),
            report_type=row.get("report_type"),
            raw_json=row.to_dict(),
            source_system="FEC",
            ingestion_timestamp=datetime.now(UTC),
        )

        session.add(bronze_record)
        loaded_count += 1

    # Commit the page
    session.commit()

    if duplicate_count > 0:
        logger.info(f"Skipped {duplicate_count} duplicate records")

    logger.info(f"Loaded {loaded_count} new records to bronze layer")
    return loaded_count


@task(cache_policy=NO_CACHE)
def update_extraction_state(
    session: Session,
    committee_id: str,
    election_cycle: int,
    last_contribution_date: date,
    last_sub_id: str,
    records_loaded: int,
    page_number: int,
    is_complete: bool = False,
) -> None:
    """
    Update or create extraction state.

    Args:
        session: Database session
        committee_id: FEC committee ID
        election_cycle: Election cycle year
        last_contribution_date: Date of last processed contribution
        last_sub_id: Sub ID of last processed contribution
        records_loaded: Number of records loaded in this run
        page_number: Last page processed
        is_complete: Whether extraction is complete
    """
    logger = get_run_logger()

    # Get or create state
    stmt = select(BronzeFECExtractionState).where(
        BronzeFECExtractionState.committee_id == committee_id,
        BronzeFECExtractionState.election_cycle == election_cycle,
    )

    state = session.execute(stmt).scalar_one_or_none()

    if state:
        state.last_contribution_date = last_contribution_date
        state.last_sub_id = last_sub_id
        state.total_contributions_extracted += records_loaded
        state.last_extraction_timestamp = datetime.now(UTC)
        state.last_page_processed = page_number
        state.is_complete = is_complete
    else:
        state = BronzeFECExtractionState(
            committee_id=committee_id,
            election_cycle=election_cycle,
            last_contribution_date=last_contribution_date,
            last_sub_id=last_sub_id,
            total_contributions_extracted=records_loaded,
            last_extraction_timestamp=datetime.now(UTC),
            last_page_processed=page_number,
            is_complete=is_complete,
        )
        session.add(state)

    session.commit()
    logger.info(
        f"Updated extraction state: page={page_number}, total={state.total_contributions_extracted}"
    )


@flow(name="FEC to Bronze", log_prints=True)
def fec_to_bronze_flow(
    state: USState,
    election_cycle: int,
    committee_id: str | None = None,
) -> dict[str, int]:
    """
    Extract FEC data and load to bronze layer with incremental processing.

    Args:
        state: US state code
        election_cycle: Election cycle year
        committee_id: Specific committee ID (if None, processes all state candidates)

    Returns:
        Dictionary with extraction statistics
    """
    logger = get_run_logger()
    election_cycle = validate_election_cycle(election_cycle)

    logger.info(f"Starting FEC to Bronze flow for {state.value}, cycle {election_cycle}")

    extractor = FECAPIExtractor()
    total_loaded = 0
    committees_processed = 0

    with get_db_session() as session:
        # Get committees to process
        if committee_id:
            committees = [{"committee_id": committee_id}]
        else:
            committees = extractor.get_candidate_committees(state, election_cycle)

        logger.info(f"Processing {len(committees)} committees")

        for committee_info in committees:
            cid = committee_info["committee_id"]
            logger.info(f"Processing committee: {cid}")

            # Get extraction state
            extraction_state = get_extraction_state(session, cid, election_cycle)

            # Determine start date
            if extraction_state:
                # Incremental load: start from last date - lookback days
                start_date = extractor.calculate_lookback_date(
                    extraction_state.last_contribution_date
                )
                starting_page = extraction_state.last_page_processed + 1
                logger.info(f"Incremental load from {start_date}, page {starting_page}")
            else:
                # Initial load: get all data
                start_date = None
                starting_page = 1
                logger.info("Initial load - fetching all historical data")

            # Extract and load page by page
            committee_total = 0

            for page_df, metadata in extractor.extract_schedule_a_pages(
                committee_id=cid,
                election_cycle=election_cycle,
                start_date=start_date,
                starting_page=starting_page,
            ):
                # Load this page
                loaded = load_page_to_bronze(session, page_df, cid, election_cycle)
                committee_total += loaded
                total_loaded += loaded

                # Update state after each page
                if not page_df.empty:
                    last_row = page_df.iloc[-1]
                    last_contribution_date = last_row["contribution_receipt_date"]
                    last_sub_id = str(last_row["sub_id"])

                    # Convert to date if it's a datetime/timestamp
                    if hasattr(last_contribution_date, "date"):
                        last_contribution_date = last_contribution_date.date()

                    update_extraction_state(
                        session=session,
                        committee_id=cid,
                        election_cycle=election_cycle,
                        last_contribution_date=last_contribution_date,
                        last_sub_id=last_sub_id,
                        records_loaded=loaded,
                        page_number=metadata["page"],
                        is_complete=(metadata["page"] >= metadata["total_pages"]),
                    )

                logger.info(
                    f"Committee {cid}: Page {metadata['page']}/{metadata['total_pages']} - "
                    f"{loaded} loaded, {committee_total} total"
                )

            committees_processed += 1
            logger.info(f"Completed committee {cid}: {committee_total} total records loaded")

    logger.info(
        f"Flow complete: {committees_processed} committees, {total_loaded} total records loaded"
    )

    return {
        "committees_processed": committees_processed,
        "total_records_loaded": total_loaded,
    }
