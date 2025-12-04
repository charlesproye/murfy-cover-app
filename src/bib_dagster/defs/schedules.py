import dagster as dg

from bib_dagster.defs.spark_jobs import MAKE_PARTITIONS
from bib_dagster.defs.spark_jobs.a_asset_resp_to_raw import response_to_raw_tss
from bib_dagster.defs.spark_jobs.b_asset_raw_to_pph import raw_ts_to_pph
from bib_dagster.defs.spark_jobs.c_asset_pph_to_rph import raw_pph_to_rph
from bib_dagster.defs.spark_jobs.d_asset_phases_to_results_weeks import (
    phases_to_results_weeks,
)
from bib_dagster.defs.spark_jobs.e_asset_results import refresh_results_models

# Jobs for individual stages - useful for running single partitions
response_to_raw_tss_job = dg.define_asset_job(
    name="response_to_raw_tss_job",
    selection=dg.AssetSelection.assets(response_to_raw_tss),
    partitions_def=MAKE_PARTITIONS,
)

raw_ts_to_pph_job = dg.define_asset_job(
    name="raw_ts_to_pph_job",
    selection=dg.AssetSelection.assets(raw_ts_to_pph),
    partitions_def=MAKE_PARTITIONS,
)

raw_pph_to_rph_job = dg.define_asset_job(
    name="raw_pph_to_rph_job",
    selection=dg.AssetSelection.assets(raw_pph_to_rph),
    partitions_def=MAKE_PARTITIONS,
)

results_pipeline_job = dg.define_asset_job(
    name="results_pipeline_job",
    selection=dg.AssetSelection.assets(phases_to_results_weeks, refresh_results_models),
)

# Job that runs the entire pipeline (all three assets in order)
full_pipeline_job = dg.define_asset_job(
    name="full_pipeline_job",
    selection=dg.AssetSelection.assets(
        response_to_raw_tss, raw_ts_to_pph, raw_pph_to_rph
    ),
    partitions_def=MAKE_PARTITIONS,
)


@dg.schedule(cron_schedule="0 1 * * *", job=full_pipeline_job)
def full_pipeline_schedule():
    """Run the full pipeline (response -> raw -> pph -> rph) weekly on Monday for all makes."""
    for make in MAKE_PARTITIONS.get_partition_keys():
        yield dg.RunRequest(run_key=make, partition_key=make)


@dg.multi_asset_sensor(
    monitored_assets=[dg.AssetKey("raw_pph_to_rph")],
    job=results_pipeline_job,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def results_pipeline_sensor(context: dg.MultiAssetSensorEvaluationContext):
    """Trigger results pipeline (phases_to_results_weeks -> refresh_results_models) after all raw_pph_to_rph partitions are materialized.

    This sensor monitors the partitioned raw_pph_to_rph asset and triggers
    the results pipeline once all partitions have been materialized.
    """
    # Get all partition keys and check which ones have new materializations
    asset_key = dg.AssetKey("raw_pph_to_rph")
    all_partitions = set(MAKE_PARTITIONS.get_partition_keys())
    materialized_partitions = set()

    for partition_key, events in context.latest_materialization_records_by_partition(
        asset_key
    ).items():
        if events is not None:
            materialized_partitions.add(partition_key)

    # Only trigger if all partitions have been materialized
    if materialized_partitions >= all_partitions:
        # Advance the cursor to avoid re-triggering
        context.advance_all_cursors()
        yield dg.RunRequest(run_key="all_partitions_complete")


@dg.schedule(cron_schedule="0 0 * * *", job=response_to_raw_tss_job)
def full_pipeline_response_to_raw_tss_schedule():
    """Run the response to raw tss job for all makes."""
    for make in MAKE_PARTITIONS.get_partition_keys():
        yield dg.RunRequest(run_key=make, partition_key=make)
