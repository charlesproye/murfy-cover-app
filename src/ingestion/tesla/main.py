import argparse
import schedule
import time
import logging
from dotenv import load_dotenv
from data_fetcher import fetch_all_vehicle_ids, job
from utils import setup_logging
import json
from s3_handler import compress_data

# Global variables to store account information
accounts_info = []

def main():
    setup_logging()
    load_dotenv()

    parser = argparse.ArgumentParser(description="Fetch and save Tesla vehicle data.")
    parser.add_argument("--accounts", required=True, type=str, help="JSON string containing account information")
    args = parser.parse_args()

    global accounts_info
    accounts_info = json.loads(args.accounts)

    for account in accounts_info:
        access_token_key = account['access_token_key']
        refresh_token_key = account['refresh_token_key']
        professional_account = account.get('professional_account', False)
        vehicle_id = account.get('vehicle_id')

        if professional_account:
            vehicle_ids = fetch_all_vehicle_ids(access_token_key, refresh_token_key)
        else:
            vehicle_ids = [vehicle_id]

        account['vehicle_ids'] = vehicle_ids
        schedule_vehicle_jobs(vehicle_ids, access_token_key, refresh_token_key)

    schedule.every().day.at("00:00").do(compression_task)

    while True:
        schedule.run_pending()
        time.sleep(1)

def schedule_vehicle_jobs(vehicle_ids, access_token_key, refresh_token_key):
    for vehicle_id in vehicle_ids:
        schedule.every(240).seconds.do(
            job, vehicle_id, access_token_key, refresh_token_key
        ).tag(vehicle_id)

def compression_task():
    logging.info("Starting compression task")
    schedule.clear()
    compress_data()
    logging.info("Compression task completed")
    
    # Reschedule all vehicle jobs
    for account in accounts_info:
        schedule_vehicle_jobs(account['vehicle_ids'], account['access_token_key'], account['refresh_token_key'])

if __name__ == "__main__":
    main()
