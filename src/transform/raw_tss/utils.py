from datetime import datetime, timedelta

def get_next_scheduled_timestamp(reference_ts_str, data):
    """
    Get the next scheduled timestamp for a given reference timestamp and data.
    Args:
        reference_ts_str (str): The reference timestamp in ISO format.
        data (dict): The data containing the weekday, time, and other information.
    Returns:
        str: The next scheduled timestamp in ISO format.
    """
    # Parse the reference timestamp
    reference_ts = datetime.fromisoformat(reference_ts_str.replace("Z", "+00:00"))

    # Mapping of weekdays to integers
    weekday_map = {
        'monday': 0, 'tuesday': 1, 'wednesday': 2,
        'thursday': 3, 'friday': 4, 'saturday': 5, 'sunday': 6
    }

    target_weekday = weekday_map[data['weekday'].lower()]
    target_hour = data['time']['hour']
    target_minute = data['time']['minute']

    # Start with the current week's target day
    days_ahead = (target_weekday - reference_ts.weekday()) % 7
    candidate_day = reference_ts.date() + timedelta(days=days_ahead)

    # Build the candidate datetime
    candidate_ts = datetime.combine(candidate_day, datetime.min.time(), tzinfo=reference_ts.tzinfo)
    candidate_ts = candidate_ts.replace(hour=target_hour, minute=target_minute)

    # If the candidate is not strictly after the reference, go to next week
    if candidate_ts <= reference_ts:
        candidate_ts += timedelta(days=7)

    return candidate_ts.isoformat()
