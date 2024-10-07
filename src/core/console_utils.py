from argparse import ArgumentParser, REMAINDER
import ast
from typing import Callable

from pandas import DataFrame as DF
from rich.traceback import install as install_rich_traceback
from rich import print

from core.pandas_utils import total_MB_memory_usage

def parse_kwargs(cli_args: dict[str, dict[str, str]] = [], **kwargs):
    # Set up argparse to accept known arguments
    parser = ArgumentParser(**kwargs)
    
    # Add mandatory arguments
    for cli_arg_name, cli_arg_properties in cli_args.items():
        parser.add_argument(cli_arg_name, **cli_arg_properties)
    
    # Parse known arguments
    known_args, unknown_args = parser.parse_known_args()

    # Convert known_args (Namespace) to a dictionary
    known_args_dict = vars(known_args)

    # Convert known_args (Namespace) to a dictionary and filter out None values
    known_args_dict = {k: v for k, v in vars(known_args).items() if v is not None}

    # Parse unknown arguments manually
    parsed_kwargs = {}
    for item in unknown_args:
        # Split the argument on the first "=" to separate the key and the value
        item_tokens = item.split("=", 1)
        if len(item_tokens) == 1:
            parsed_kwargs[item.lstrip('-')] = True
            continue

        key, value_str = item_tokens
        key = key.lstrip('-')
        # Use ast.literal_eval to safely evaluate the value string
        try:
            value = ast.literal_eval(value_str)
        except (ValueError, SyntaxError):
            # Fallback to using the string directly if evaluation fails
            value = value_str
        parsed_kwargs[key] = value

    # Combine known_args_dict with parsed_kwargs
    return {**parsed_kwargs, **known_args_dict}

def main_decorator(main_func):
    """
    ### Description:
    This decorator calls the rich.traceback install function to get better looking tracebacks.
    It also catches KeyboardInterrupt exception to quit properly.
    """
    def wrapper(*args, **kwargs):  # Accept arbitrary positional and keyword arguments
        install_rich_traceback(extra_lines=0, width=130)
        try:
            main_func(*args, **kwargs)  # Pass the arguments to the original function
        except KeyboardInterrupt:
            print("[blue]KeyboardInterrupt, exiting...")
    return wrapper

@main_decorator
def single_dataframe_script_main(dataframe_gen: Callable[[bool], DF], **kwargs):
    df:DF = dataframe_gen(**kwargs)
    print(df)
    print("all columns:")
    print(df.dtypes)
    print(f"total memory usage: {total_MB_memory_usage(df):.2f}MB.")
