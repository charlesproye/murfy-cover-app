from argparse import ArgumentParser, REMAINDER
import ast
import logging

# from rich import print
from rich.traceback import install as install_rich_traceback

logger = logging.getLogger(__name__)

def get_src_dst_pairs_args(description: str=""):
    # arguments parsning
    parser = ArgumentParser(description=description)
    parser.add_argument("file_pairs", nargs='+', help="Pairs of read and write file paths. Each pair must consist of a read path followed by a write path.")
    args = parser.parse_args()

    if len(args.file_pairs) % 2 != 0:
        parser.error("File paths must be provided in pairs of two: a read file followed by a write file.")
    if len(args.file_pairs) < 2:
        parser.error("At least one pair of read and write file paths must be provided.")
        
    ext_ref_files = args.file_pairs[::2]
    dest_files = args.file_pairs[1::2]

    return ext_ref_files, dest_files

def parse_kwargs(mandatory_args: list = [], optional_args: dict = {}, **kwargs):
    # Set up argparse to accept known arguments
    parser = ArgumentParser(**kwargs)
    
    # Add mandatory arguments
    for arg in mandatory_args:
        parser.add_argument(f'--{arg}', required=True)
    
    # Add optional arguments
    for arg, default in optional_args.items():
        parser.add_argument(f"--{arg}", default=default)
    
    # Parse known arguments
    known_args, unknown_args = parser.parse_known_args()

    # Convert known_args (Namespace) to a dictionary
    known_args_dict = vars(known_args)

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
    def wrapper():
        install_rich_traceback(extra_lines=0, width=130)
        try:
            main_func()
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt, exiting...")
    return wrapper
