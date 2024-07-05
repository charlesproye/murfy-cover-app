from argparse import ArgumentParser, REMAINDER
import ast

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

def parse_kwargs() -> dict:
    # Set up argparse to accept known arguments
    parser = ArgumentParser(description="Example script.")
    # Use nargs and REMAINDER to accept an arbitrary number of unknown arguments
    parser.add_argument('kwargs', nargs=REMAINDER, help="Keyword arguments")

    # Parse known arguments
    _, str_kwargs = parser.parse_known_args()

    kwargs = {}
    for item in str_kwargs:
        # Split the argument on the first "=" to separate the key and the value
        item_tokens = item.split("=", 1)
        if len(item_tokens) == 1:
            kwargs[item.lstrip('-')] = True
            continue

        key, value_str = item_tokens
        key = item.lstrip('-')
        # Use ast.literal_eval to safely evaluate the value string
        try:
            value = ast.literal_eval(value_str)
        except (ValueError, SyntaxError):
            # Fallback to using the string directly if evaluation fails
            value = value_str
        kwargs[key.lstrip('-')] = value
    return kwargs
