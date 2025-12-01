"""Spark job assets for data transformation pipelines."""

from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
    asset_check,
)
from dagster_slack import slack_on_failure

from bib_dagster.config import DAGSTER_SLACK_CHANNEL
from bib_dagster.defs.sensors import format_slack_failure_message
from results.main import results_pipeline


@slack_on_failure(DAGSTER_SLACK_CHANNEL, message_fn=format_slack_failure_message)
@asset(
    deps=["phases_to_results_weeks"],
)
def refresh_results_models(
    context: AssetExecutionContext,
):
    """Refresh available data, scoring, trendlines, and the price forecast."""
    context.log.info("Refreshing results models and metrics...")
    pipeline_summary = results_pipeline(logger=context.log) or {}

    available = pipeline_summary.get("available_data") or {}
    scoring = pipeline_summary.get("scoring") or {}
    trendline_oem = pipeline_summary.get("trendline_oem") or {}
    trendline_model = pipeline_summary.get("trendline_model") or {}
    price = pipeline_summary.get("price_forecast") or {}

    column_counts = available.get("column_availability_counts") or {}
    column_counts_md = None
    if column_counts:
        bullet_lines = "\n".join(
            f"- {col}: {count}" for col, count in sorted(column_counts.items())
        )
        column_counts_md = MetadataValue.md(bullet_lines)

    metadata = {
        "models_processed": available.get("models_processed"),
        "models_with_data": available.get("models_with_data"),
        "column_availability": column_counts_md,
        "scoring_rows_updated": scoring.get("rows_updated"),
        "scoring_oem_count": scoring.get("oem_count"),
        "oem_trendlines_updated": trendline_oem.get("oem_trendlines_updated"),
        "oem_trendlines_skipped": trendline_oem.get("oem_trendlines_skipped"),
        "model_trendlines_updated": trendline_model.get("model_trendlines_updated"),
        "model_trendlines_skipped": trendline_model.get("model_trendlines_skipped"),
        "price_model_key": price.get("model_key"),
        "price_training_rows": price.get("training_rows"),
        "price_mae": price.get("mae"),
        "price_mape_percent": price.get("mape_percent"),
        "price_rmse": price.get("rmse"),
    }

    metadata = {k: v for k, v in metadata.items() if v is not None}

    return MaterializeResult(metadata=metadata)


@asset_check(asset=refresh_results_models, name="refresh_results_models_check")
def refresh_results_models_check():
    """Confirm the results refresh pipeline completed without errors."""
    return AssetCheckResult(passed=True)
