import logging
import os
import sys
from urllib.parse import quote_plus

from celery import Celery
from celery.signals import setup_logging, worker_init

logger = logging.getLogger(__name__)


@setup_logging.connect
def config_loggers(*args, **kwargs):
    """Configure logging for Celery workers to show application logs."""
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s: %(levelname)s/%(processName)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


redis_host = os.environ.get("REDIS_HOST", "redis")
redis_port = os.environ.get("REDIS_PORT", "6379")
redis_password = os.environ.get("REDIS_PASSWORD")

if redis_password:
    safe_password = quote_plus(redis_password)
    REDIS_URL = f"redis://:{safe_password}@{redis_host}:{redis_port}/0"
else:
    REDIS_URL = f"redis://{redis_host}:{redis_port}/0"

# SMTP Settings - optional at import time, validated at worker startup
SMTP_EMAIL: str | None = os.environ.get("SMTP_EMAIL")
SMTP_USER: str | None = os.environ.get("SMTP_USER")
SMTP_PASSWORD: str | None = os.environ.get("SMTP_PASSWORD")
SMTP_HOST: str | None = os.environ.get("SMTP_HOST")
SMTP_PORT: int | None = (
    int(os.environ["SMTP_PORT"]) if "SMTP_PORT" in os.environ else None
)


@worker_init.connect
def validate_smtp_config(**kwargs):
    """Validate that all required SMTP environment variables are set when worker starts."""
    required_smtp_vars = {
        "SMTP_EMAIL": SMTP_EMAIL,
        "SMTP_USER": SMTP_USER,
        "SMTP_PASSWORD": SMTP_PASSWORD,
        "SMTP_HOST": SMTP_HOST,
        "SMTP_PORT": SMTP_PORT,
    }

    missing = [name for name, value in required_smtp_vars.items() if value is None]

    if missing:
        error_msg = f"Missing required SMTP environment variables: {', '.join(missing)}"
        logger.error(error_msg)
        sys.exit(1)

    logger.info(f"SMTP configured: {SMTP_USER}@{SMTP_HOST}:{SMTP_PORT}")


if redis_password:
    logger.info(f"Using Redis URL: {REDIS_URL.replace(safe_password, '***')}")
else:
    logger.info(f"Using Redis URL: {REDIS_URL}")

# --- CELERY APP ---
celery_app = Celery(
    "premium_report",
    broker=REDIS_URL,
    backend=REDIS_URL,
)

# --- CONFIGURATION CELERY ---
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=300,
    task_soft_time_limit=240,
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=50,
    result_expires=3600,
)

# --- ROUTING ---
celery_app.conf.task_routes = {
    "reports.workers.tasks.*": {"queue": "pdf_generation"},
}

# --- AUTODISCOVERY ---
celery_app.autodiscover_tasks(["reports.workers"])

# Import task modules to ensure they are registered
from reports.workers import flash_report_task, tasks  # noqa: E402, F401
