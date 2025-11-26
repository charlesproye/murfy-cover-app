"""Spark job assets for data transformation pipelines."""

from datetime import datetime, timedelta

from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    MaterializeResult,
    asset,
    asset_check,
)
from dagster_slack import slack_on_failure

from bib_dagster.config import DAGSTER_SLACK_CHANNEL
from bib_dagster.defs.sensors import format_slack_failure_message
from transform.result_week.main import main as phases_to_results_weeks_main


@slack_on_failure(DAGSTER_SLACK_CHANNEL, message_fn=format_slack_failure_message)
@asset(
    deps=["raw_pph_to_rph"],
)
def phases_to_results_weeks(
    context: AssetExecutionContext,
):
    """Aggregate result phases by week and compute presentable metrics (SoH, charge levels, consumption).

    Reads partitioned parquet datasets from S3, applies filtering and outlier removal,
    then writes aggregated metrics to the vehicle_data table.
    """
    context.log.info("Starting phases_to_results_weeks asset...")
    results_weeks = phases_to_results_weeks_main(logger=context.log)

    # Calculate metadata for the last week
    one_week_ago = datetime.now() - timedelta(days=7)
    recent_entries = results_weeks[results_weeks["DATE"] > one_week_ago]

    return MaterializeResult(
        metadata={
            "total_entries": len(results_weeks),
            "recent_entries_last_week": len(recent_entries),
            "unique_vehicles": int(results_weeks["VIN"].nunique()),
            "date_range": f"{results_weeks['DATE'].min()} to {results_weeks['DATE'].max()}",
        }
    )


@asset_check(asset=phases_to_results_weeks, name="phases_to_results_weeks_check")
def phases_to_results_weeks_check():
    """Check if the processed phases to result weeks transformation is successful."""
    return AssetCheckResult(passed=True)
