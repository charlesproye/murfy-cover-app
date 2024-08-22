import argparse
import logging
import os
import textwrap

import dotenv
from ingestion.mobilisights.ingester import MobilisightsIngester


def main():
    dotenv.load_dotenv()
    LOG_LEVEL = os.getenv("LOG_LEVEL", default="INFO")
    LOG_FILE = os.getenv("LOG_FILE")
    if LOG_FILE is None:
        logging.basicConfig(
            level=logging.getLevelNamesMapping().get(LOG_LEVEL),
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        )
    else:
        logging.basicConfig(
            filename=LOG_FILE,
            level=logging.getLevelNamesMapping().get(LOG_LEVEL),
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        )

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent("""\
        Ingest data from the High Mobility API into an S3 bucket

        environment variables:
          HM_BASE_URL:          HM API base URL
          HM_CLIENT_ID:         HM OAuth client ID
          HM_CLIENT_SECRET:     HM OAuth client secret
          S3_ENDPOINT           S3 base endpoint
          S3_REGION             S3 region
          S3_BUCKET:            S3 bucket to save the info
          S3_KEY:               S3 access key id
          S3_SECRET:            S3 secret access key
        """),
    )
    parser.add_argument(
        "--rate_limit",
        type=int,
        default=36,
        help="interval (in minutes) at which to query vehicle info",
    )
    parser.add_argument(
        "--max_workers",
        type=int,
        default=8,
        help="maximum number of threads to process the vehicles info (mostly limited by S3)",
    )
    parser.add_argument(
        "--compress_interval",
        type=str,
        default=12,
        help="interval (in hours) at which to compress S3 data",
    )

    parser.add_argument(
        "--compress_threaded",
        action=argparse.BooleanOptionalAction,
        help="run the compresser in threaded mode",
    )
    args = parser.parse_args()

    ingester = MobilisightsIngester(
        rate_limit=args.rate_limit,
        max_workers=args.max_workers,
        compress_interval=args.compress_interval,
    )

    ingester.run()


if __name__ == "__main__":
    main()

