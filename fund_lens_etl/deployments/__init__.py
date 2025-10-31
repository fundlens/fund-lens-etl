"""Prefect deployment configurations for FundLens ETL pipeline."""

from fund_lens_etl.deployments.schedules import (
    create_all_deployments,
    create_bronze_deployments,
    create_gold_deployments,
    create_silver_deployments,
)

__all__ = [
    "create_bronze_deployments",
    "create_silver_deployments",
    "create_gold_deployments",
    "create_all_deployments",
]
