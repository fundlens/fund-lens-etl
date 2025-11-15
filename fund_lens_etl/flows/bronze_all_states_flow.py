"""
Bronze ingestion flow for all US states.

This flow loops through all states and runs the bronze ingestion flow for each.
It's designed for:
1. One-time full load (all states, full_refresh=True)
2. Daily incremental processing (all states, full_refresh=False)
"""

from datetime import date
from typing import TypedDict

from prefect import flow, get_run_logger

from fund_lens_etl.flows.bronze_ingestion_flow import bronze_ingestion_flow
from fund_lens_etl.models import USState


class PipelineSummary(TypedDict):
    """Type definition for pipeline summary statistics."""

    total_states: int
    successful_states: int
    failed_states: list[str]
    total_committees: int
    total_candidates: int
    total_contributions: int


@flow(name="bronze-ingestion-all-states")
def bronze_all_states_flow(
    election_cycle: int,
    start_date: date | None = None,
    end_date: date | None = None,
    full_refresh: bool = False,
) -> PipelineSummary:
    """
    Run bronze ingestion for all US states.

    This flow processes all 51 states (50 states + DC) sequentially.
    Each state is processed independently, so if one fails, others continue.

    Args:
        election_cycle: Election cycle year (e.g., 2026)
        start_date: Optional start date for contributions
        end_date: Optional end date for contributions
        full_refresh: If True, performs full refresh for all states.
                     If False, uses incremental mode with 90-day lookback.

    Returns:
        Dictionary with summary statistics:
        - total_states: Number of states processed
        - successful_states: Number of states that completed successfully
        - failed_states: List of states that failed
        - total_committees: Total committees across all states
        - total_candidates: Total candidates across all states
        - total_contributions: Total contributions across all states
    """
    logger = get_run_logger()

    # Summary statistics
    summary: PipelineSummary = {
        "total_states": 0,
        "successful_states": 0,
        "failed_states": [],
        "total_committees": 0,
        "total_candidates": 0,
        "total_contributions": 0,
    }

    # Get all states from the enum
    all_states = list(USState)
    logger.info(
        f"Starting bronze ingestion for {len(all_states)} states "
        f"(cycle: {election_cycle}, full_refresh: {full_refresh})"
    )

    # Process each state sequentially
    for state in all_states:
        summary["total_states"] += 1
        logger.info(f"Processing state {summary['total_states']}/{len(all_states)}: {state.value}")

        try:
            # Run bronze ingestion for this state
            result = bronze_ingestion_flow(
                state=state,
                election_cycle=election_cycle,
                start_date=start_date,
                end_date=end_date,
                full_refresh=full_refresh,
            )

            # Aggregate statistics
            summary["successful_states"] += 1
            summary["total_committees"] += result.get("committees_count", 0)
            summary["total_candidates"] += result.get("candidates_count", 0)
            summary["total_contributions"] += result.get("contributions_count", 0)

            logger.info(
                f"✓ Completed {state.value}: "
                f"{result.get('committees_count', 0)} committees, "
                f"{result.get('candidates_count', 0)} candidates, "
                f"{result.get('contributions_count', 0)} contributions"
            )

        except Exception as e:
            # Log error but continue with other states
            summary["failed_states"].append(state.value)
            logger.error(f"✗ Failed processing {state.value}: {e}")
            continue

    # Final summary
    logger.info("=" * 80)
    logger.info("ALL STATES PROCESSING COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Total states processed: {summary['total_states']}")
    logger.info(f"Successful: {summary['successful_states']}")
    logger.info(f"Failed: {len(summary['failed_states'])}")
    if summary["failed_states"]:
        logger.warning(f"Failed states: {', '.join(summary['failed_states'])}")
    logger.info(f"Total committees: {summary['total_committees']:,}")
    logger.info(f"Total candidates: {summary['total_candidates']:,}")
    logger.info(f"Total contributions: {summary['total_contributions']:,}")
    logger.info("=" * 80)

    return summary
