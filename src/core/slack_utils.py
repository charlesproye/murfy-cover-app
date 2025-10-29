# Slack utility functions for sending messages to Slack channels.

import os

import requests


def send_slack_message(channel_id: str, text: str):
    """Send a message to a Slack channel."""
    slack_token = os.getenv("SLACK_TOKEN")
    if not slack_token:
        raise ValueError("SLACK_TOKEN is not set")
    headers = {
        "Authorization": f"Bearer {slack_token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    payload = {"channel": channel_id, "text": text}
    r = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers=headers,
        json=payload,
        timeout=10,
    )
    if r.status_code == 200:
        print("Message envoyé !")
        print(r.json().get("message", {}).get("text"))
    else:
        print("Body text:", r.text)

