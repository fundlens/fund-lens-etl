"""
Prefect deployment schedules for FundLens ETL pipeline.

This module defines scheduled deployments for:
- Bronze ingestion (daily incremental + monthly full refresh)
- Silver transformation (triggered after bronze completes)
- Gold transformation (triggered after silver completes)
"""

from prefect.client.schemas.schedules import CronSchedule

from fund_lens_etl.config import USState

# ============================================================================
# Deployment Parameters
# ============================================================================

# Default parameters for all deployments
DEFAULT_STATE = USState.MD
DEFAULT_CYCLE = 2026

# Timezone for scheduling (FEC operates on US Eastern Time)
TIMEZONE = "America/New_York"


# ============================================================================
# Bronze Ingestion Deployments
# ============================================================================


def create_bronze_deployments():
    """
    Create and deploy bronze ingestion flows.

    Returns the number of deployments created.
    """
    from fund_lens_etl.flows.bronze_ingestion_flow import bronze_ingestion_flow

    # Daily incremental extraction (weekdays only)
    bronze_ingestion_flow.deploy(
        name="bronze-ingestion-daily-incremental",
        work_pool_name="default",
        parameters={
            "state": DEFAULT_STATE,
            "election_cycle": DEFAULT_CYCLE,
            "full_refresh": False,
        },
        schedules=[
            CronSchedule(cron="0 2 * * 1-5", timezone=TIMEZONE)  # 2 AM ET, Monday-Friday
        ],
        tags=["etl", "bronze", "incremental", "daily"],
        description="Daily incremental extraction from FEC API with 90-day lookback",
        version="1.0.0",
    )

    # Monthly full refresh (first Sunday of each month)
    bronze_ingestion_flow.deploy(
        name="bronze-ingestion-monthly-full-refresh",
        work_pool_name="default",
        parameters={
            "state": DEFAULT_STATE,
            "election_cycle": DEFAULT_CYCLE,
            "full_refresh": True,
        },
        schedules=[
            CronSchedule(cron="0 3 1-7 * 0", timezone=TIMEZONE)  # 3 AM ET, first Sunday of month
        ],
        tags=["etl", "bronze", "full-refresh", "monthly"],
        description="Monthly full refresh to catch late filings and corrections",
        version="1.0.0",
    )

    return 2


# ============================================================================
# Silver Transformation Deployments
# ============================================================================


def create_silver_deployments():
    """
    Create and deploy silver transformation flows.

    Returns the number of deployments created.
    """
    from fund_lens_etl.flows.silver_transformation_flow import silver_transformation_flow

    # Daily silver transformation (runs after bronze ingestion)
    silver_transformation_flow.deploy(
        name="silver-transformation-daily",
        work_pool_name="default",
        parameters={
            "state": DEFAULT_STATE.value,
            "cycle": DEFAULT_CYCLE,
        },
        schedules=[
            CronSchedule(
                cron="30 2 * * 1-5", timezone=TIMEZONE
            )  # 2:30 AM ET, Monday-Friday (30 min after bronze)
        ],
        tags=["etl", "silver", "transformation", "daily"],
        description="Daily transformation of bronze data to silver layer",
        version="1.0.0",
    )

    # Monthly silver transformation (after monthly full refresh)
    silver_transformation_flow.deploy(
        name="silver-transformation-monthly",
        work_pool_name="default",
        parameters={
            "state": DEFAULT_STATE.value,
            "cycle": DEFAULT_CYCLE,
        },
        schedules=[
            CronSchedule(
                cron="30 3 1-7 * 0", timezone=TIMEZONE
            )  # 3:30 AM ET, first Sunday of month (30 min after full refresh)
        ],
        tags=["etl", "silver", "transformation", "monthly"],
        description="Monthly transformation after full refresh",
        version="1.0.0",
    )

    return 2


# ============================================================================
# Gold Transformation Deployments
# ============================================================================


def create_gold_deployments():
    """
    Create and deploy gold transformation flows.

    Returns the number of deployments created.
    """
    from fund_lens_etl.flows.gold_transformation_flow import gold_transformation_flow

    # Daily gold transformation (runs after silver transformation)
    gold_transformation_flow.deploy(
        name="gold-transformation-daily",
        work_pool_name="default",
        parameters={
            "state": DEFAULT_STATE.value,
            "cycle": DEFAULT_CYCLE,
        },
        schedules=[
            CronSchedule(
                cron="30 3 * * 1-5", timezone=TIMEZONE
            )  # 3:30 AM ET, Monday-Friday (1 hour after silver)
        ],
        tags=["etl", "gold", "analytics", "daily"],
        description="Daily transformation of silver data to gold analytics layer",
        version="1.0.0",
    )

    # Monthly gold transformation (after monthly silver)
    gold_transformation_flow.deploy(
        name="gold-transformation-monthly",
        work_pool_name="default",
        parameters={
            "state": DEFAULT_STATE.value,
            "cycle": DEFAULT_CYCLE,
        },
        schedules=[
            CronSchedule(
                cron="30 4 1-7 * 0", timezone=TIMEZONE
            )  # 4:30 AM ET, first Sunday of month (1 hour after monthly silver)
        ],
        tags=["etl", "gold", "analytics", "monthly"],
        description="Monthly transformation after full refresh pipeline",
        version="1.0.0",
    )

    return 2


# ============================================================================
# Full Pipeline Deployment
# ============================================================================


def create_all_deployments():
    """
    Create all deployment configurations for the complete ETL pipeline.

    Returns:
        Total number of deployments created
    """
    total = 0
    total += create_bronze_deployments()
    total += create_silver_deployments()
    total += create_gold_deployments()
    return total


# ============================================================================
# Deployment Schedule Summary
# ============================================================================


def print_schedule_summary():
    """Print a human-readable summary of all deployment schedules."""
    print("=" * 80)
    print("FundLens ETL Pipeline - Deployment Schedules")
    print("=" * 80)
    print()
    print("DAILY PIPELINE (Monday-Friday):")
    print("  2:00 AM ET - Bronze: Incremental ingestion (90-day lookback)")
    print("  2:30 AM ET - Silver: Transform bronze → silver")
    print("  3:30 AM ET - Gold: Transform silver → gold")
    print()
    print("MONTHLY PIPELINE (First Sunday of month):")
    print("  3:00 AM ET - Bronze: Full refresh (all data)")
    print("  3:30 AM ET - Silver: Transform bronze → silver")
    print("  4:30 AM ET - Gold: Transform silver → gold")
    print()
    print("CONFIGURATION:")
    print(f"  State: {DEFAULT_STATE.value}")
    print(f"  Cycle: {DEFAULT_CYCLE}")
    print(f"  Timezone: {TIMEZONE}")
    print("=" * 80)
