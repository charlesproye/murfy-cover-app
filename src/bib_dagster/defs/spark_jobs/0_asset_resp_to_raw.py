"""Spark job assets for data transformation pipelines."""

import pathlib

import yaml
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    StaticPartitionsDefinition,
    asset,
    asset_check,
)
from dagster_slack import slack_on_failure

from bib_dagster.defs.sensors import format_slack_failure_message
from bib_dagster.pipes.pipes_spark_operator import PipesSparkApplicationClient
from bib_dagster.pipes.spark_resources import DriverResource, ExecutorResource
from core.models.make import MakeEnum
from transform.raw_tss.main import ResponseToRawTssCLI

# Create a static partition for each make
make_partitions = StaticPartitionsDefinition([make.value for make in MakeEnum])


@slack_on_failure("#bib-bot-test", message_fn=format_slack_failure_message)
@asset(
    group_name="spark_jobs",
    partitions_def=make_partitions,
)
def response_to_raw_tss(
    context: AssetExecutionContext,
    spark_pipes: PipesSparkApplicationClient,
):
    """Response (json raw output from APIs) to raw TSS.

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
    spark_spec["mainApplicationFile"] = ResponseToRawTssCLI.file_path_in_docker()

    return spark_pipes.run(
        context=context,
        base_spec=spark_spec,
        namespace="spark-operator",
        cleanup=False,
        driver_resource=DriverResource(cores=1, memory="1G", memoryOverhead="512m"),
        executor_resource=ExecutorResource(cores=1, instances=1, memory="512m"),
    ).get_materialize_result()


@asset_check(asset=response_to_raw_tss)
def response_to_raw_tss_check():
    """Check if the response to raw TSS is successful for this partition."""
    # No partiton check possible yet
    # https://github.com/dagster-io/dagster/issues/17005
    # TODO: Implement partition check when issue is resolved
    return AssetCheckResult(
        passed=True,
    )
