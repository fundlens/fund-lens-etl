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
    Create deployment configurations for bronze ingestion flow.

    Returns two deployments:
    1. Daily incremental extraction (2 AM ET, Mon-Fri)
    2. Monthly full refresh (3 AM ET, 1st of month)
    """
    from fund_lens_etl.flows.bronze_ingestion_flow import bronze_ingestion_flow

    deployments = []

    # Daily incremental extraction (weekdays only)
    daily_incremental = bronze_ingestion_flow.to_deployment(
        name="bronze-ingestion-daily-incremental",
        version="1.0.0",
        description="Daily incremental extraction from FEC API with 90-day lookback",
        tags=["etl", "bronze", "incremental", "daily"],
        work_pool_name="default",
        pull_steps=[],  # Code is already on the filesystem, no need to pull
        parameters={
            "state": DEFAULT_STATE,
            "election_cycle": DEFAULT_CYCLE,
            "full_refresh": False,
        },
        schedule=CronSchedule(
            cron="0 2 * * 1-5",  # 2 AM ET, Monday-Friday
            timezone=TIMEZONE,
        ),
    )
    deployments.append(daily_incremental)

    # Monthly full refresh (first Sunday of each month)
    monthly_full_refresh = bronze_ingestion_flow.to_deployment(
        name="bronze-ingestion-monthly-full-refresh",
        version="1.0.0",
        description="Monthly full refresh to catch late filings and corrections",
        tags=["etl", "bronze", "full-refresh", "monthly"],
        work_pool_name="default",
        pull_steps=[],  # Code is already on the filesystem, no need to pull
        parameters={
            "state": DEFAULT_STATE,
            "election_cycle": DEFAULT_CYCLE,
            "full_refresh": True,
        },
        schedule=CronSchedule(
            cron="0 3 1-7 * 0",  # 3 AM ET, first Sunday of month
            timezone=TIMEZONE,
        ),
    )
    deployments.append(monthly_full_refresh)

    return deployments


# ============================================================================
# Silver Transformation Deployments
# ============================================================================


def create_silver_deployments():
    """
    Create deployment configurations for silver transformation flow.

    Returns one deployment that runs after bronze ingestion completes.
    Scheduled to run 30 minutes after daily bronze ingestion.
    """
    from fund_lens_etl.flows.silver_transformation_flow import silver_transformation_flow

    deployments = []

    # Daily silver transformation (runs after bronze ingestion)
    daily_silver = silver_transformation_flow.to_deployment(
        name="silver-transformation-daily",
        version="1.0.0",
        description="Daily transformation of bronze data to silver layer",
        tags=["etl", "silver", "transformation", "daily"],
        work_pool_name="default",
        pull_steps=[],  # Code is already on the filesystem, no need to pull
        parameters={
            "state": DEFAULT_STATE.value,
            "cycle": DEFAULT_CYCLE,
        },
        schedule=CronSchedule(
            cron="30 2 * * 1-5",  # 2:30 AM ET, Monday-Friday (30 min after bronze)
            timezone=TIMEZONE,
        ),
    )
    deployments.append(daily_silver)

    # Monthly silver transformation (after monthly full refresh)
    monthly_silver = silver_transformation_flow.to_deployment(
        name="silver-transformation-monthly",
        version="1.0.0",
        description="Monthly transformation after full refresh",
        tags=["etl", "silver", "transformation", "monthly"],
        work_pool_name="default",
        pull_steps=[],  # Code is already on the filesystem, no need to pull
        parameters={
            "state": DEFAULT_STATE.value,
            "cycle": DEFAULT_CYCLE,
        },
        schedule=CronSchedule(
            cron="30 3 1-7 * 0",  # 3:30 AM ET, first Sunday of month (30 min after full refresh)
            timezone=TIMEZONE,
        ),
    )
    deployments.append(monthly_silver)

    return deployments


# ============================================================================
# Gold Transformation Deployments
# ============================================================================


def create_gold_deployments():
    """
    Create deployment configurations for gold transformation flow.

    Returns one deployment that runs after silver transformation completes.
    Scheduled to run 1 hour after daily silver transformation.
    """
    from fund_lens_etl.flows.gold_transformation_flow import gold_transformation_flow

    deployments = []

    # Daily gold transformation (runs after silver transformation)
    daily_gold = gold_transformation_flow.to_deployment(
        name="gold-transformation-daily",
        version="1.0.0",
        description="Daily transformation of silver data to gold analytics layer",
        tags=["etl", "gold", "analytics", "daily"],
        work_pool_name="default",
        pull_steps=[],  # Code is already on the filesystem, no need to pull
        parameters={
            "state": DEFAULT_STATE.value,
            "cycle": DEFAULT_CYCLE,
        },
        schedule=CronSchedule(
            cron="30 3 * * 1-5",  # 3:30 AM ET, Monday-Friday (1 hour after silver)
            timezone=TIMEZONE,
        ),
    )
    deployments.append(daily_gold)

    # Monthly gold transformation (after monthly silver)
    monthly_gold = gold_transformation_flow.to_deployment(
        name="gold-transformation-monthly",
        version="1.0.0",
        description="Monthly transformation after full refresh pipeline",
        tags=["etl", "gold", "analytics", "monthly"],
        work_pool_name="default",
        pull_steps=[],  # Code is already on the filesystem, no need to pull
        parameters={
            "state": DEFAULT_STATE.value,
            "cycle": DEFAULT_CYCLE,
        },
        schedule=CronSchedule(
            cron="30 4 1-7 * 0",  # 4:30 AM ET, first Sunday of month (1 hour after monthly silver)
            timezone=TIMEZONE,
        ),
    )
    deployments.append(monthly_gold)

    return deployments


# ============================================================================
# Full Pipeline Deployment
# ============================================================================


def create_all_deployments():
    """
    Create all deployment configurations for the complete ETL pipeline.

    Returns:
        List of all deployment configurations (bronze, silver, gold)
    """
    deployments = []
    deployments.extend(create_bronze_deployments())
    deployments.extend(create_silver_deployments())
    deployments.extend(create_gold_deployments())
    return deployments


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
