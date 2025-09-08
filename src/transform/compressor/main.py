import argparse
import logging
import sys
from transform.compressor.config import COMPRESSORS
from core.console_utils import main_decorator
import asyncio



@main_decorator
def main(make: str):

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    )
    
    asyncio.run(COMPRESSORS[make].compress(make))


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="Script de compression avec un argument OEM requis.")
    parser.add_argument("arg", type=str, help="")
    args = parser.parse_args()

    if args.arg in COMPRESSORS.keys():
        main(args.arg)
    else:
        raise ValueError(f"Argument not found or OEM not supported.")
