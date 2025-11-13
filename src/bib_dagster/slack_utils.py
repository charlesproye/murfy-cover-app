import os
from collections.abc import Sequence

from dagster import (
    AssetCheckEvaluation,
    DagsterEventType,
    EventLogRecord,
    HookContext,
    RunStatusSensorContext,
)
from dagster._core.events.log import EventLogEntry
from dagster_slack import SlackResource


def build_and_post_slack_message_if_asset_check_failed(
    event_record: EventLogRecord, slack: SlackResource
):
    event_log_entry: EventLogEntry = event_record.event_log_entry
    dagster_event: AssetCheckEvaluation = event_log_entry.dagster_event

    job_name = event_log_entry.job_name
    run_id = event_log_entry.run_id
    asset_key = dagster_event.event_specific_data.asset_key.to_user_string()
    check_name = dagster_event.event_specific_data.check_name
    asset_check_passed = dagster_event.event_specific_data.passed
    asset_check_severity = dagster_event.event_specific_data.metadata["status"].text

    if not asset_check_passed:
        severity_emoji = ":warning:" if asset_check_severity == "warn" else ":x:"
        instance_base_url = os.environ["DAGIT_BASE_URL"]
        slack_alerting_channel = os.environ["SLACK_CHANNEL"]

        slack_message_body = [
            {
                "color": "#E9D502" if asset_check_severity == "warn" else "#FF0000",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f"{severity_emoji} Asset Check Failed",
                        },
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*Asset Check:*\n `{check_name}`",
                            },
                            {"type": "mrkdwn", "text": f"*Asset:*\n `{asset_key}`"},
                        ],
                    },
                    {
                        "type": "section",
                        "fields": [{"type": "mrkdwn", "text": f"*Job:*\n`{job_name}`"}],
                    },
                    {
                        "type": "actions",
                        "elements": [
                            {
                                "type": "button",
                                "text": {
                                    "type": "plain_text",
                                    "text": "View in Dagster UI",
                                },
                                "value": "dagster_button",
                                "url": instance_base_url + "/runs/" + run_id,
                            }
                        ],
                    },
                ],
            }
        ]

        slack.get_client().chat_postMessage(
            channel=slack_alerting_channel, attachments=slack_message_body
        )


def format_slack_failure_message(context: HookContext) -> str:
    """Format a Slack message for run failures including asset checks."""
    job_name = f"{context.job_name} (*{context.step_key}*)"
    run_id = context.run_id

    partition_key = (
        context._step_execution_context.run_tags.get("dagster/partition")
        if context._step_execution_context
        else None
    )

    error_msg = (
        context.op_exception.__repr__() if context.op_exception else "Unknown error"
    )

    emoji = ":x:"
    failure_type = "Run Failed"

    return (
        f"{emoji} *{failure_type}*\n"
        + f"*Job:* {job_name}\n"
        + f"*Run ID:* <http://{os.getenv('DAGSTER_UI_URL', 'localhost:3000')}/runs/{run_id}|{run_id}>\n"
        + (f"*Partition Key:* {partition_key}\n" if partition_key else "")
        + f"*Error:* `{error_msg}`\n"
    )


def handle_asset_check_status(context: RunStatusSensorContext, slack: SlackResource):
    run_id = context.dagster_run.run_id
    event_records: Sequence[EventLogRecord] = context.instance.get_records_for_run(
        run_id,
        of_type=DagsterEventType.ASSET_CHECK_EVALUATION,
    ).records

    for event_record in event_records:
        build_and_post_slack_message_if_asset_check_failed(event_record, slack)
