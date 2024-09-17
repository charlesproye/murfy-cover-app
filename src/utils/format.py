

from datetime import datetime, timedelta
import pytz
from logging import Logger
from apscheduler.triggers.base import BaseTrigger





def format_timedelta(td):
    # Extract days, hours, minutes and seconds
    days, seconds = td.days, td.seconds
    hours = days * 24 + seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60

    # Build the formatted string based on the duration
    parts = []
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes:02}m")
    if seconds:
        parts.append(f"{seconds:02}s")

    # Join the parts or return "0s" if the duration is 0
    return ''.join(parts) if parts else "0s"


def log_process(logger : Logger, trigger : BaseTrigger):
    now = datetime.now(tz=pytz.UTC)
    next_fire_time = trigger.get_next_fire_time(None, now)
    fire_delay = next_fire_time - now
    
    formated_fire_delay = format_timedelta(fire_delay)

    green = "\x1b[38;5;46m"
    yellow = "\x1b[38;5;226m"
    red = "\x1b[38;5;196m"
    reset = "\x1b[0m"

    if fire_delay < timedelta(minutes=15):
        color = green
    elif fire_delay < timedelta(minutes=60):
        color = yellow
    else:
        color = red

    if next_fire_time is None:
        logger.info("Will not run")
    else:
        logger.info(f"Next fire in {color}{formated_fire_delay}{reset}")
