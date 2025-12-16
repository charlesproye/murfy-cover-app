import os

DAGSTER_SLACK_CHANNEL = os.getenv("DAGSTER_SLACK_CHANNEL", "#bib-bot-test")
DAGSTER_BASE_URL = os.getenv("DAGSTER_BASE_URL", "http://localhost:3000")
SLACK_TOKEN = os.getenv("SLACK_TOKEN", "")
