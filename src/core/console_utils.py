from argparse import ArgumentParser
import ast
from typing import Callable

from pandas import DataFrame as DF
from rich.traceback import install as install_rich_traceback
from rich import print

from core.pandas_utils import total_MB_memory_usage, sanity_check

def parse_kwargs(cli_args: dict[str, dict[str, str]] = [], **kwargs):
    parser = ArgumentParser(**kwargs)                                                                 # Set up argparse to accept known arguments
    for cli_arg_name, cli_arg_properties in cli_args.items():                                         # Add mandatory arguments
        parser.add_argument(cli_arg_name, **cli_arg_properties)
    known_args, unknown_args = parser.parse_known_args()                                              # Parse known arguments
    known_args_dict = vars(known_args)                                                                # Convert known_args (Namespace) to a dictionary
    known_args_dict = {k: v for k, v in vars(known_args).items() if v is not None}                    # Convert known_args (Namespace) to a dictionary and filter out None values
    print(known_args_dict)
    parsed_kwargs = {}                                                                                # Parse unknown arguments manually
    for item in unknown_args:
        item_tokens = item.split("=", 1)                                                              # Split the argument on the first "=" to separate the key and the value
        if len(item_tokens) == 1:
            parsed_kwargs[item.lstrip('-')] = True
            continue
        key, value_str = item_tokens
        key = key.lstrip('-')
        try:
            value = ast.literal_eval(value_str)                                                       # Use ast.literal_eval to safely evaluate the value string
        except (ValueError, SyntaxError):
            value = value_str                                                                         # Fallback to using the string directly if evaluation fails
        parsed_kwargs[key] = value

    print(parsed_kwargs)
    return {**known_args_dict, **parsed_kwargs}                                                       # Combine known_args_dict with parsed_kwargs

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
            exit()
    return wrapper

@main_decorator
def single_dataframe_script_main(dataframe_gen: Callable[[bool], DF], logger=None, **kwargs) -> DF:
    df:DF = dataframe_gen(**kwargs)
    if logger:
        logger.debug(df)
        logger.debug("sanity check:")
        logger.debug(sanity_check(df))
        logger.debug(f"total memory usage: {total_MB_memory_usage(df):.2f}MB.")
    else:
        print(df)
        print("sanity check:")
        print(sanity_check(df))
        print(f"total memory usage: {total_MB_memory_usage(df):.2f}MB.")

    return df
