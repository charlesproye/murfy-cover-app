from datetime import datetime, timedelta, timezone

def get_next_scheduled_timestamp(reference_ts, data):
    """
    Get the next scheduled timestamp for a given reference timestamp and Row data.
    Returns a Python datetime.datetime object (NOT string).
    """

    if not data or not hasattr(data, "weekday") or not hasattr(data, "time"):
        return reference_ts

    if isinstance(reference_ts, str):
        reference_ts = datetime.fromisoformat(reference_ts.replace("Z", "+00:00"))

    if reference_ts.tzinfo is None:
        reference_ts = reference_ts.replace(tzinfo=timezone.utc)

    weekday_map = {
        'monday': 0, 'tuesday': 1, 'wednesday': 2,
        'thursday': 3, 'friday': 4, 'saturday': 5, 'sunday': 6
    }

    target_weekday = weekday_map.get(data.weekday.lower())
    if target_weekday is None:
        return reference_ts

    target_hour = getattr(data.time, "hour", None)
    target_minute = getattr(data.time, "minute", None)
    if target_hour is None or target_minute is None:
        return reference_ts

    days_ahead = (target_weekday - reference_ts.weekday()) % 7
    candidate_day = reference_ts.date() + timedelta(days=days_ahead)

    candidate_ts = datetime.combine(candidate_day, datetime.min.time(), tzinfo=reference_ts.tzinfo)
    candidate_ts = candidate_ts.replace(hour=target_hour, minute=target_minute)

    if candidate_ts + timedelta(hours=2) <= reference_ts:
        candidate_ts += timedelta(days=7)

    return candidate_ts
