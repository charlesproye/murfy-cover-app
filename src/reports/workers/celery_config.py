import logging
import os
from urllib.parse import quote_plus

from celery import Celery

logger = logging.getLogger(__name__)

redis_host = os.environ.get("REDIS_HOST", "redis")
redis_port = os.environ.get("REDIS_PORT", "6379")
redis_password = os.environ.get("REDIS_PASSWORD")

if redis_password:
    safe_password = quote_plus(redis_password)
    REDIS_URL = f"redis://:{safe_password}@{redis_host}:{redis_port}/0"
else:
    REDIS_URL = f"redis://{redis_host}:{redis_port}/0"

logger.info(
    f"Using Redis URL: {REDIS_URL.replace(safe_password, '***') if redis_password else REDIS_URL}"
)

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
