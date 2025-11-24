"""
Prefect deployment schedules for FundLens ETL pipeline.

This module defines scheduled deployments for:
- Bronze ingestion (daily incremental for ALL states, M-F at 1 AM)
- Silver transformation (triggered after bronze completes)
- Gold transformation (triggered after silver completes)
"""

from prefect.client.schemas.schedules import CronSchedule
from prefect.events import DeploymentEventTrigger

# ============================================================================
# Deployment Parameters
# ============================================================================

# Default parameters for all deployments
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
    from fund_lens_etl.flows.bronze_all_states_flow import bronze_all_states_flow

    # Daily incremental extraction for ALL states (weekdays only at 1 AM)
    bronze_all_states_flow.from_source(
        source="/opt/fund-lens-etl",
        entrypoint="fund_lens_etl/flows/bronze_all_states_flow.py:bronze_all_states_flow",
    ).deploy(
        name="bronze-ingestion-all-states-daily",
        work_pool_name="default",
        parameters={
            "election_cycle": DEFAULT_CYCLE,
            "full_refresh": False,
        },
        schedules=[
            CronSchedule(cron="0 1 * * 1-5", timezone=TIMEZONE)  # 1 AM ET, Monday-Friday
        ],
        tags=["etl", "bronze", "incremental", "daily", "all-states"],
        description="Daily incremental extraction for all 51 states with 1-day lookback (M-F at 1 AM)",
        version="3.0.0",
    )

    return 1


# ============================================================================
# Silver Transformation Deployments
# ============================================================================


def create_silver_deployments():
    """
    Create and deploy silver transformation flows.

    Returns the number of deployments created.
    """
    from fund_lens_etl.flows.silver_transformation_flow import silver_transformation_flow

    # Silver transformation triggered by EITHER bronze ingestion OR monthly bulk reconciliation
    silver_transformation_flow.from_source(
        source="/opt/fund-lens-etl",
        entrypoint="fund_lens_etl/flows/silver_transformation_flow.py:silver_transformation_flow",
    ).deploy(
        name="silver-transformation-all-states-triggered",
        work_pool_name="default",
        parameters={
            "state": None,  # None = process all states
            "cycle": DEFAULT_CYCLE,
        },
        triggers=[
            DeploymentEventTrigger(
                expect={"prefect.flow-run.Completed"},
                match_related={"prefect.resource.name": "bronze-ingestion-all-states-daily"},
            ),
            DeploymentEventTrigger(
                expect={"prefect.flow-run.Completed"},
                match_related={"prefect.resource.name": "monthly-bulk-reconciliation-2026"},
            ),
        ],
        tags=["etl", "silver", "transformation", "triggered", "all-states"],
        description="Silver transformation triggered by bronze daily ingestion OR monthly bulk reconciliation",
        version="3.0.0",
    )

    return 1


# ============================================================================
# Gold Transformation Deployments
# ============================================================================


def create_gold_deployments():
    """
    Create and deploy gold transformation flows.

    Returns the number of deployments created.
    """
    from fund_lens_etl.flows.gold_transformation_flow import gold_transformation_flow

    # Gold transformation triggered by silver transformation completion
    gold_transformation_flow.from_source(
        source="/opt/fund-lens-etl",
        entrypoint="fund_lens_etl/flows/gold_transformation_flow.py:gold_transformation_flow",
    ).deploy(
        name="gold-transformation-all-states-triggered",
        work_pool_name="default",
        parameters={
            "state": None,  # None = process all states
            "cycle": DEFAULT_CYCLE,
        },
        triggers=[
            DeploymentEventTrigger(
                expect={"prefect.flow-run.Completed"},
                match_related={
                    "prefect.resource.name": "silver-transformation-all-states-triggered"
                },
            )
        ],
        tags=["etl", "gold", "analytics", "triggered", "all-states"],
        description="Gold transformation triggered on silver transformation completion",
        version="3.0.0",
    )

    return 1


# ============================================================================
# Monthly Bulk Reconciliation Deployment
# ============================================================================


def create_monthly_reconciliation_deployment():
    """
    Create and deploy monthly bulk reconciliation flow.

    Returns the number of deployments created.
    """
    from fund_lens_etl.flows.monthly_bulk_reconciliation_flow import (
        monthly_bulk_reconciliation_flow,
    )

    # Monthly bulk reconciliation on first Saturday at 1 AM
    # This will trigger Silver → Gold just like daily incremental
    monthly_bulk_reconciliation_flow.from_source(
        source="/opt/fund-lens-etl",
        entrypoint="fund_lens_etl/flows/monthly_bulk_reconciliation_flow.py:monthly_bulk_reconciliation_flow",
    ).deploy(
        name="monthly-bulk-reconciliation-2026",
        work_pool_name="default",
        parameters={
            "election_cycle": DEFAULT_CYCLE,
            "cleanup_after": True,
        },
        schedules=[
            # First Saturday of each month at 1 AM ET
            # Day 1-7 that is Saturday (day 6)
            CronSchedule(cron="0 1 1-7 * 6", timezone=TIMEZONE)
        ],
        tags=["etl", "bulk", "reconciliation", "monthly"],
        description="Monthly bulk data reconciliation on first Saturday at 1 AM (triggers Silver → Gold)",
        version="3.0.0",
    )

    return 1


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
    total += create_monthly_reconciliation_deployment()
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
    print("WEEKDAY PIPELINE (Monday-Friday) - ALL 51 STATES:")
    print("  1:00 AM ET - Bronze: Incremental ingestion (1-day lookback)")
    print("               Runtime: ~30-60 minutes")
    print("               ↓")
    print("  [TRIGGERED] Silver: Transform bronze → silver")
    print("               Runtime: ~30-60 minutes")
    print("               ↓")
    print("  [TRIGGERED] Gold: Transform silver → gold")
    print("               Runtime: ~30-60 minutes")
    print()
    print("MONTHLY RECONCILIATION (First Saturday) - ALL 51 STATES:")
    print("  1:00 AM ET - Bulk: Download + reconcile (full data)")
    print("               Runtime: ~2-3 hours")
    print("               ↓")
    print("  [TRIGGERED] Silver: Transform bronze → silver")
    print("               Runtime: ~30-60 minutes")
    print("               ↓")
    print("  [TRIGGERED] Gold: Transform silver → gold")
    print("               Runtime: ~30-60 minutes")
    print()
    print("CONFIGURATION:")
    print("  States: All 51 (50 states + DC)")
    print(f"  Cycle: {DEFAULT_CYCLE}")
    print(f"  Timezone: {TIMEZONE}")
    print("  Total Deployments: 4")
    print()
    print("TRIGGER CHAINS:")
    print("  Daily:   Bronze → Silver → Gold (M-F)")
    print("  Monthly: Bulk Reconciliation → Silver → Gold (1st Saturday)")
    print("  Silver accepts triggers from BOTH Bronze and Bulk flows")
    print("=" * 80)
