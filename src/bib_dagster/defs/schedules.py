import dagster as dg

from bib_dagster.defs.spark_jobs import MAKE_PARTITIONS
from bib_dagster.defs.spark_jobs.a_asset_resp_to_raw import response_to_raw_tss
from bib_dagster.defs.spark_jobs.b_asset_raw_to_pph import raw_ts_to_pph
from bib_dagster.defs.spark_jobs.c_asset_pph_to_rph import raw_pph_to_rph

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

# Job that runs the entire pipeline (all three assets in order)
full_pipeline_job = dg.define_asset_job(
    name="full_pipeline_job",
    selection=dg.AssetSelection.assets(
        response_to_raw_tss, raw_ts_to_pph, raw_pph_to_rph
    ),
    partitions_def=MAKE_PARTITIONS,
)


@dg.schedule(cron_schedule="0 1 * * 1", job=full_pipeline_job)
def full_pipeline_schedule():
    """Run the full pipeline (response -> raw -> pph -> rph) weekly on Monday for all makes."""
    for make in MAKE_PARTITIONS:
        yield dg.RunRequest(run_key=make, partition_key=make)


@dg.schedule(cron_schedule="0 0 * * *", job=response_to_raw_tss_job)
def full_pipeline_response_to_raw_tss_schedule():
    """Run the response to raw tss job for all makes."""
    for make in MAKE_PARTITIONS:
        yield dg.RunRequest(run_key=make, partition_key=make)
