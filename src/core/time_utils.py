"""
Provides:  
1. A contextmanager `time_to_exec` that can be used to measure the execution time of a task (must provide the name of the task).  
1. A dictionary `time_dict` that stores all the sum of the execution time for each task.  
1. A `print_time_dict` function to print all the execution times on te CLI.  
"""
import time
from contextlib import contextmanager
from datetime import datetime, timedelta


time_dict = {}

@contextmanager
def time_to_exec(message:str, print_msg=False):
    """
    ### Description:
    Measures the time to execute a code block.  
    The execution duration is stored in the global `time_dict` variable.  
    ### Args:
    - message: "Name" of the code block that will be used as a key in `time_dict`.
    - print_msg: If True, prints the execution time write away preceeded by the message.
    """
    global time_dict
    
    start_time = time.time()  # Record the start time
    yield  # Yield control to the block of code
    end_time = time.time()  # Record the end time after the block has executed
    execution_time = end_time - start_time  # Calculate the execution time
        
    if print_msg:
        print(f"{message}: {execution_time}s")  # Print the execution time with the message

    if not message in time_dict:
        time_dict[message] = execution_time
    else:
        time_dict[message] += execution_time

def print_time_dict() -> dict:
    """
    Prints all the "{message}: {execution_time}" key/value pairs in time_dict.
    """
    global time_dict
    for msg, execution_time in time_dict.items():
        print(f"{msg}: {execution_time}s")
    return time_dict



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
