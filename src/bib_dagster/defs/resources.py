"""Resource definitions for Dagster."""

import os

import dagster as dg
from dagster_slack import SlackResource

from bib_dagster.defs.sensors import slack_failure_notifications
from bib_dagster.pipes.pipes_spark_operator import PipesSparkApplicationClient


@dg.definitions
def resources():
    """Define Dagster resources.

    The spark_pipes resource automatically handles both environments:
    - DAGSTER_ENV=dev (or unset): Runs locally with Python PySpark
    - DAGSTER_ENV=prod: Runs on Kubernetes with Spark Operator

    The slack resource is configured with SLACK_TOKEN environment variable
    for sending notifications to Slack channels.
    """
    return dg.Definitions(
        resources={
            "spark_pipes": PipesSparkApplicationClient(
                load_incluster_config=None,
                poll_interval=10.0,
                forward_termination=True,
                # Inject Pipes context via environment variables
                inject_method="env",
            ),
            "slack": SlackResource(token=os.getenv("SLACK_TOKEN", "")),
        },
        sensors=[slack_failure_notifications],
    )
