import argparse
import asyncio
import time
import os
import logging
from dotenv import load_dotenv
from data_fetcher import fetch_all_vehicle_ids, job
from data_utils import setup_logging
import json
from s3_handler import compress_data

# Global variables to store account information
accounts_info = []

async def main():
    setup_logging()
    load_dotenv()

    if os.getenv("TESLA_COMPRESS") == "1":
        await compression_task()
        logging.info("Compression task enabled")
    else:
        logging.info("Compression task disabled")

    parser = argparse.ArgumentParser(description="Fetch and save Tesla vehicle data.")
    parser.add_argument("--accounts", required=True, type=str, help="JSON string containing account information")
    args = parser.parse_args()

    global accounts_info
    accounts_info = json.loads(args.accounts)

    tasks = []
    for account in accounts_info:
        access_token_key = account['access_token_key']
        refresh_token_key = account['refresh_token_key']
        professional_account = account.get('professional_account', False)
        vehicle_id = account.get('vehicle_id')

        if professional_account:
            vehicle_ids = await fetch_all_vehicle_ids(access_token_key, refresh_token_key)
        else:
            vehicle_ids = [vehicle_id]

        account['vehicle_ids'] = vehicle_ids
        tasks.extend([process_vehicle(vid, access_token_key, refresh_token_key) for vid in vehicle_ids])

    await asyncio.gather(*tasks)

async def process_vehicle(vehicle_id, access_token_key, refresh_token_key):
    while True:
        await job(vehicle_id, access_token_key, refresh_token_key)
        await asyncio.sleep(240)

async def compression_task():
    logging.info("Starting compression task")
    await compress_data()
    logging.info("Compression task completed")

if __name__ == "__main__":
    asyncio.run(main())
