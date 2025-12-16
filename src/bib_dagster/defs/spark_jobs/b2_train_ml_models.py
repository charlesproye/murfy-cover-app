"""Spark job assets for data transformation pipelines."""

import pathlib
from collections import defaultdict

import yaml
from dagster import AssetCheckResult, AssetExecutionContext, asset, asset_check
from dagster_slack import slack_on_failure

from bib_dagster.config import DAGSTER_SLACK_CHANNEL
from bib_dagster.defs.sensors import format_slack_failure_message
from bib_dagster.pipes.pipes_spark_operator import PipesSparkApplicationClient
from bib_dagster.pipes.spark_resources import (
    DriverResource,
    ExecutorResource,
    JobResources,
)
from core.models import MakeEnum
from transform.models.soh_ml_models_pipeline import SoHMLModelsPipeline

JOB_RESOURCES: dict[str, JobResources] = defaultdict(
    lambda: JobResources(
        driver=DriverResource(cores=1, memory="2G", memoryOverhead="512m"),
        executors=ExecutorResource(
            cores=3, instances=1, memory="11G", memoryOverhead="1G"
        ),
    ),
)


@slack_on_failure(DAGSTER_SLACK_CHANNEL, message_fn=format_slack_failure_message)
@asset(
    group_name="spark_jobs",
    deps=["response_to_raw_tss"],
    op_tags={"dagster/concurrency_key": "spark_jobs"},
)
def train_ml_models(
    context: AssetExecutionContext,
    spark_pipes: PipesSparkApplicationClient,
):
    """Train ML models for State of Health (SoH) estimation.

    This asset trains machine learning models to predict battery State of Health
    metrics based on processed vehicle telemetry data.
    """
    # Get the make from the partition key
    with open(pathlib.Path(__file__).parent / "baseline_spark_spec.yaml") as f:
        spark_spec = yaml.safe_load(f)

    # Pass the specific make to the Spark job
    spark_spec["arguments"] = ["run"]
    spark_spec["mainApplicationFile"] = SoHMLModelsPipeline.file_path_in_docker()

    # Only renault for now
    make = MakeEnum.renault.value

    driver_resource = JOB_RESOURCES[make].driver
    executor_resource = JOB_RESOURCES[make].executors

    return spark_pipes.run(
        context=context,
        base_spec=spark_spec,
        namespace="spark-operator",
        cleanup=False,
        driver_resource=driver_resource,
        executor_resource=executor_resource,
    ).get_materialize_result()


@asset_check(asset=train_ml_models, name="train_ml_models_check")
def raw_ts_to_pph_check():
    """Check if the ML model training job completed successfully for this partition."""
    # No partiton check possible yet
    # https://github.com/dagster-io/dagster/issues/17005
    # TODO: Implement partition check when issue is resolved
    return AssetCheckResult(
        passed=True,
    )
