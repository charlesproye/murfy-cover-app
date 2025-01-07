import time
from contextlib import contextmanager

time_dict = {}

@contextmanager
def time_to_exec(message:str, print_msg=False):
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
    global time_dict
    for msg, execution_time in time_dict.items():
        print(f"{msg}: {execution_time}s")
    return time_dict
