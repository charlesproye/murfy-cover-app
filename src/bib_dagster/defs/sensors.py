"""Sensors for monitoring Dagster runs and sending notifications."""

import os

from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunStatusSensorContext,
    run_status_sensor,
)
from dagster_slack import SlackResource, make_slack_on_run_failure_sensor

from bib_dagster.slack_utils import (
    format_slack_failure_message,
    handle_asset_check_status,
)

# Create a Slack notification sensor for run failures
# This will catch both asset materialization failures and asset check failures
slack_failure_notifications = make_slack_on_run_failure_sensor(
    channel="#bib-bot-test",
    slack_token=os.getenv("SLACK_TOKEN"),
    text_fn=format_slack_failure_message,
    name="slack_failure_notifications",
)


# https://github.com/dagster-io/dagster/discussions/21281#discussion-6522956
@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE, default_status=DefaultSensorStatus.RUNNING
)
def failed_asset_check_in_failed_job_sensor(
    context: RunStatusSensorContext, slack: SlackResource
):
    handle_asset_check_status(context, slack)
