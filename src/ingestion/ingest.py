import argparse
import logging
import os
import textwrap

import dotenv
from ingestion.high_mobility_ingester import HMIngester

if __name__ == "__main__":
    dotenv.load_dotenv()
    LOG_LEVEL = os.getenv("LOG_LEVEL") or "INFO"
    logging.basicConfig(
        level=logging.getLevelNamesMapping().get(LOG_LEVEL),
        format="%(asctime)s - %(levelname)s - %(message)s",
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
        "--refresh_interval",
        type=int,
        help="interval (in seconds) at which to refresh the list of clearances",
    )
    parser.add_argument(
        "--max_workers",
        type=int,
        help="maximum number of threads to fetch the vehicles info (mostly limited by S3)",
    )
    args = parser.parse_args()

    ingester: HMIngester

    print(args)
    match args.refresh_interval, args.max_workers:
        case None, None:
            ingester = HMIngester()
        case interval, None:
            ingester = HMIngester(refresh_interval=interval)
        case None, workers:
            ingester = HMIngester(max_workers=workers)
        case _:
            ingester = HMIngester(
                refresh_interval=args.refresh_interval, max_workers=args.max_workers
            )

    ingester.run()

