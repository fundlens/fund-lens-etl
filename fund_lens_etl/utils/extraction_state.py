"""Utilities for managing bronze extraction state for incremental loads."""

from datetime import date, timedelta

from fund_lens_models.bronze.fec import BronzeFECExtractionState
from sqlalchemy import select
from sqlalchemy.orm import Session

from fund_lens_etl.config import get_settings


def get_extraction_state(
    session: Session,
    committee_id: str,
    election_cycle: int,
) -> BronzeFECExtractionState | None:
    """
    Get extraction state for a committee/cycle.

    Args:
        session: SQLAlchemy session
        committee_id: FEC committee ID
        election_cycle: Election cycle year

    Returns:
        Extraction state record or None if not found
    """
    stmt = select(BronzeFECExtractionState).where(
        BronzeFECExtractionState.committee_id == committee_id,
        BronzeFECExtractionState.election_cycle == election_cycle,
    )
    return session.execute(stmt).scalar_one_or_none()


def calculate_incremental_start_date(
    session: Session,
    committee_id: str,
    election_cycle: int,
    lookback_days: int | None = None,
) -> date | None:
    """
    Calculate start date for incremental extraction.

    If extraction state exists, returns last_contribution_date - lookback_days.
    If no state exists, returns None (full extraction).

    The lookback window accounts for:
    - Quarterly FEC filing deadlines (up to 45 days after quarter end)
    - Late amendments to previous filings
    - FEC data processing delays
    Default: 180 days (configured in settings)

    Args:
        session: SQLAlchemy session
        committee_id: FEC committee ID
        election_cycle: Election cycle year
        lookback_days: Days to look back (defaults to 180 from config)

    Returns:
        Start date for extraction, or None for full extraction
    """
    settings = get_settings()
    lookback_days = lookback_days or settings.lookback_days

    state = get_extraction_state(session, committee_id, election_cycle)

    if not state:
        # No previous extraction - do full load
        return None

    # Calculate lookback date
    lookback_date = state.last_contribution_date - timedelta(days=lookback_days)
    return lookback_date


def update_extraction_state(
    session: Session,
    committee_id: str,
    election_cycle: int,
    last_contribution_date: date,
    last_sub_id: str,
    total_records_extracted: int,
    extraction_start_date: date | None,
    extraction_end_date: date | None,
    is_complete: bool = True,
) -> BronzeFECExtractionState:
    """
    Update or create extraction state after successful extraction.

    Args:
        session: SQLAlchemy session
        committee_id: FEC committee ID
        election_cycle: Election cycle year
        last_contribution_date: Date of the last contribution processed
        last_sub_id: Sub ID of the last contribution processed
        total_records_extracted: Total records extracted in this run
        extraction_start_date: Start date filter used (None for full)
        extraction_end_date: End date filter used (None for no limit)
        is_complete: Whether extraction completed successfully

    Returns:
        Updated or created extraction state record
    """
    state = get_extraction_state(session, committee_id, election_cycle)

    if state:
        # Update existing state
        state.last_contribution_date = last_contribution_date
        state.last_sub_id = last_sub_id
        state.total_contributions_extracted += total_records_extracted
        state.extraction_start_date = extraction_start_date
        state.extraction_end_date = extraction_end_date
        state.is_complete = is_complete
    else:
        # Create new state
        state = BronzeFECExtractionState(
            committee_id=committee_id,
            election_cycle=election_cycle,
            last_contribution_date=last_contribution_date,
            last_sub_id=last_sub_id,
            total_contributions_extracted=total_records_extracted,
            extraction_start_date=extraction_start_date,
            extraction_end_date=extraction_end_date,
            is_complete=is_complete,
        )
        session.add(state)

    session.commit()
    return state


def get_last_contribution_info(
    session: Session,
    committee_id: str,
    election_cycle: int,
) -> tuple[date, str] | None:
    """
    Get the last contribution date and sub_id for a committee.

    Queries the bronze_fec_schedule_a table directly to find the most recent
    contribution, which is used to update extraction state.

    Args:
        session: SQLAlchemy session
        committee_id: FEC committee ID
        election_cycle: Election cycle year

    Returns:
        Tuple of (last_contribution_date, last_sub_id) or None if no contributions
    """
    from fund_lens_models.bronze.fec import BronzeFECScheduleA

    stmt = (
        select(
            BronzeFECScheduleA.contribution_receipt_date,
            BronzeFECScheduleA.sub_id,
        )
        .where(
            BronzeFECScheduleA.committee_id == committee_id,
            BronzeFECScheduleA.two_year_transaction_period == election_cycle,
            BronzeFECScheduleA.contribution_receipt_date.isnot(None),
        )
        .order_by(BronzeFECScheduleA.contribution_receipt_date.desc())
        .limit(1)
    )

    result = session.execute(stmt).first()
    if result:
        return result[0], result[1]
    return None
