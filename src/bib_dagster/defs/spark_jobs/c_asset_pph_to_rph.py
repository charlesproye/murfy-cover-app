"""Spark job assets for data transformation pipelines."""

import pathlib

import yaml
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    asset,
    asset_check,
)
from dagster_slack import slack_on_failure

from bib_dagster.config import DAGSTER_SLACK_CHANNEL
from bib_dagster.defs.sensors import format_slack_failure_message
from bib_dagster.defs.spark_jobs import MAKE_PARTITIONS
from bib_dagster.pipes.pipes_spark_operator import PipesSparkApplicationClient
from bib_dagster.pipes.spark_resources import DriverResource, ExecutorResource
from transform.result_phases.main import ProcessedPhaseToResultPhaseCLI


@slack_on_failure(DAGSTER_SLACK_CHANNEL, message_fn=format_slack_failure_message)
@asset(
    group_name="spark_jobs",
    partitions_def=MAKE_PARTITIONS,
    deps=["raw_ts_to_pph"],
    op_tags={"dagster/concurrency_key": "spark_jobs"},
)
def raw_pph_to_rph(
    context: AssetExecutionContext,
    spark_pipes: PipesSparkApplicationClient,
):
    """Processed phases to result phases : Step for adding features computed
    by BIB (consumption, SoH, charge levels, etc.)

    This asset is partitioned by vehicle make - one partition per make.
    Each partition processes data for a single vehicle manufacturer.

    This asset runs differently based on DAGSTER_ENV:
    - dev: Runs locally with direct Python imports and local Spark session
    - prod: Runs on Kubernetes using Spark Operator

    Args:
        context: Dagster execution context (includes partition_key for the make)
        spark_pipes: Spark Pipes client (environment-aware resource)
    """
    # Get the make from the partition key
    make = context.partition_key

    with open(pathlib.Path(__file__).parent / "baseline_spark_spec.yaml") as f:
        spark_spec = yaml.safe_load(f)

    # Pass the specific make to the Spark job
    spark_spec["arguments"] = ["run", make]
    spark_spec["mainApplicationFile"] = (
        ProcessedPhaseToResultPhaseCLI.file_path_in_docker()
    )

    return spark_pipes.run(
        context=context,
        base_spec=spark_spec,
        namespace="spark-operator",
        cleanup=False,
        driver_resource=DriverResource(cores=2, memory="7G", memoryOverhead="1G"),
        executor_resource=ExecutorResource(
            cores=2, memory="7G", instances=3, memoryOverhead="1G"
        ),
    ).get_materialize_result()


@asset_check(asset=raw_pph_to_rph, name="raw_pph_to_rph_check")
def raw_pph_to_rph_check():
    """Check if the processed phases to result phases transformation is successful for this partition."""
    # No partiton check possible yet
    # https://github.com/dagster-io/dagster/issues/17005
    # TODO: Implement partition check when issue is resolved
    return AssetCheckResult(
        passed=True,
    )
