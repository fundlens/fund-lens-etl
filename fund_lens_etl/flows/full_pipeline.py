"""Prefect flow for orchestrating the full bronze -> silver -> gold pipeline."""

from prefect import flow, get_run_logger

from fund_lens_etl.config import USState, validate_election_cycle
from fund_lens_etl.flows.bronze_flow import fec_to_bronze_flow
from fund_lens_etl.flows.gold_flow import silver_to_gold_flow
from fund_lens_etl.flows.silver_flow import bronze_to_silver_flow


@flow(name="Full ETL Pipeline", log_prints=True)
def full_pipeline_flow(
    state: USState,
    election_cycle: int,
    committee_id: str | None = None,
    batch_size: int = 1000,
) -> dict[str, dict[str, int]]:
    """
    Run the complete ETL pipeline: Bronze -> Silver -> Gold.

    This orchestrates all three layer transformations in sequence for
    Maryland FEC campaign finance data.

    Args:
        state: US state code
        election_cycle: Election cycle year
        committee_id: Optional specific committee ID (None = all state candidates)
        batch_size: Batch size for silver/gold processing

    Returns:
        Dictionary with statistics from each layer
    """
    logger = get_run_logger()
    election_cycle = validate_election_cycle(election_cycle)

    logger.info(f"Starting full ETL pipeline for {state.value}, cycle {election_cycle}")

    results = {}

    # Step 1: Extract from FEC API -> Bronze layer
    logger.info("=" * 60)
    logger.info("STEP 1: FEC API -> Bronze Layer")
    logger.info("=" * 60)

    bronze_results = fec_to_bronze_flow(
        state=state,
        election_cycle=election_cycle,
        committee_id=committee_id,
    )

    results["bronze"] = bronze_results
    logger.info(f"Bronze layer complete: {bronze_results['total_records_loaded']} records loaded")

    # Step 2: Transform Bronze -> Silver layer
    logger.info("=" * 60)
    logger.info("STEP 2: Bronze -> Silver Layer")
    logger.info("=" * 60)

    silver_results = bronze_to_silver_flow(
        election_cycle=election_cycle,
        committee_id=committee_id,
        batch_size=batch_size,
    )

    results["silver"] = silver_results
    logger.info(
        f"Silver layer complete: {silver_results['total_records_processed']} records processed"
    )

    # Step 3: Transform Silver -> Gold layer
    logger.info("=" * 60)
    logger.info("STEP 3: Silver -> Gold Layer")
    logger.info("=" * 60)

    gold_results = silver_to_gold_flow(
        election_cycle=election_cycle,
        committee_id=committee_id,
        batch_size=batch_size,
    )

    results["gold"] = gold_results
    logger.info(f"Gold layer complete: {gold_results['total_records_processed']} records processed")

    # Final summary
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 60)
    logger.info(f"State: {state.value}")
    logger.info(f"Election Cycle: {election_cycle}")
    logger.info(
        f"Bronze: {bronze_results['committees_processed']} committees, "
        f"{bronze_results['total_records_loaded']} records"
    )
    logger.info(
        f"Silver: {silver_results['batches_processed']} batches, "
        f"{silver_results['total_records_processed']} records"
    )
    logger.info(
        f"Gold: {gold_results['batches_processed']} batches, "
        f"{gold_results['total_records_processed']} records"
    )
    logger.info("=" * 60)

    return results


@flow(name="Incremental Update", log_prints=True)
def incremental_update_flow(
    state: USState,
    election_cycle: int,
    committee_id: str | None = None,
    batch_size: int = 1000,
) -> dict[str, dict[str, int]]:
    """
    Run incremental updates for existing data.

    This is the same as full_pipeline_flow but semantically represents
    an incremental update (leveraging the lookback window logic).

    Args:
        state: US state code
        election_cycle: Election cycle year
        committee_id: Optional specific committee ID
        batch_size: Batch size for processing

    Returns:
        Dictionary with statistics from each layer
    """
    logger = get_run_logger()
    logger.info("Running incremental update (with 90-day lookback)")

    return full_pipeline_flow(
        state=state,
        election_cycle=election_cycle,
        committee_id=committee_id,
        batch_size=batch_size,
    )
