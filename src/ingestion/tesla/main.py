import argparse
import asyncio
import time
import os
import logging
from dotenv import load_dotenv
from data_fetcher import fetch_all_vehicle_ids, job
from data_utils import setup_logging
from datetime import datetime, timedelta
import json
from s3_handler import compress_data, save_data_to_s3, consolidate_all_tesla_files

# Global variables to store account information
accounts_info = []
compression_event = asyncio.Event()
TESLA_COMPRESS = os.getenv('TESLA_COMPRESS', '0')

async def main():
    setup_logging()
    load_dotenv()

    parser = argparse.ArgumentParser(description="Fetch and save Tesla vehicle data.")
    parser.add_argument("--accounts", required=True, type=str, help="JSON string containing account information")
    args = parser.parse_args()

    global accounts_info
    accounts_info = json.loads(args.accounts)

    global compression_event
    compression_event = asyncio.Event()
    compression_event.set()  # Initialize the event as set by default

    compression_queue = asyncio.Queue()
    
    if TESLA_COMPRESS == "1":
        logging.info("Immediate compression requested")
        compression_event.clear()  # Prevent vehicle processing
        try:
            await perform_compression()
        except Exception as e:
            logging.error(f"Error during immediate compression: {str(e)}")
        finally:
            compression_event.set()
            logging.info("Immediate compression process completed")

    compression_task = asyncio.create_task(schedule_compression(compression_queue))
    vehicle_tasks = [process_vehicle(account) for account in accounts_info]
    
    await asyncio.gather(compression_task, *vehicle_tasks)


async def schedule_compression(compression_queue):
    while True:
        now = datetime.now()
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        seconds_until_midnight = (midnight - now).total_seconds()

        try:
            immediate = await asyncio.wait_for(compression_queue.get(), timeout=seconds_until_midnight)
            if immediate:
                logging.info("Performing compression")
                compression_event.clear()  # Stop vehicle processing
                try:
                    await perform_compression()
                except Exception as e:
                    logging.error(f"Error during compression: {str(e)}")
                finally:
                    compression_event.set()  # Resume vehicle processing
                    logging.info("Compression process completed")
        except asyncio.TimeoutError:
            logging.info("Starting scheduled midnight compression")
            compression_event.clear()  # Stop vehicle processing
            try:
                await perform_compression()
            except Exception as e:
                logging.error(f"Error during scheduled compression: {str(e)}")
            finally:
                # Don't set compression_event here, we'll do it at 6 AM
                logging.info("Scheduled compression process completed")
            
            # Wait until 6 AM
            now = datetime.now()
            five_am = now.replace(hour=5, minute=0, second=0, microsecond=0)
            if now > five_am:
                five_am += timedelta(days=1)
            await asyncio.sleep((five_am - now).total_seconds())
            
            compression_event.set()  # Resume vehicle processing at 5 AM
            logging.info("Resuming vehicle processing at 5 AM")

async def perform_compression():
    logging.info("Starting compression task")
    try:
        await compress_data()
        logging.info("Compression completed successfully")
    except Exception as e:
        logging.error(f"Error during compression: {str(e)}")
        raise
    finally:
        logging.info("Compression task finished")

async def process_vehicle(account):
    access_token_key = account['access_token_key']
    refresh_token_key = account.get('refresh_token_key', None)
    professional_account = account.get('professional_account', False)
    excel_url = account.get('excel_url', None)
    vehicle_id = account.get('vehicle_id')
    auth_code = account.get('code', None)
    if professional_account:
        vehicle_ids = await fetch_all_vehicle_ids(access_token_key, refresh_token_key, excel_url, auth_code)
    else:
        vehicle_ids = [vehicle_id]

    logging.info(f"Processing vehicles: {vehicle_ids} for account {access_token_key}")

    while True:
        await compression_event.wait()  # Wait if compression is in progress

        now = datetime.now()
        if now.hour < 5:
            logging.info("It's between midnight and 5 AM, pausing vehicle processing")
            await asyncio.sleep((now.replace(hour=5, minute=0, second=0, microsecond=0) - now).total_seconds())
            continue
        
        logging.info("Starting vehicle processing cycle")
        for vid in vehicle_ids:
            if not compression_event.is_set():
                logging.info("Compression started, pausing vehicle processing")
                break
            
            try:
                await job(vid, access_token_key, refresh_token_key, auth_code)
            except Exception as e:
                logging.error(f"Error processing vehicle {vid}: {str(e)}")
            
            if not compression_event.is_set():
                logging.info("Compression started during vehicle processing, breaking loop")
                break
        
        if compression_event.is_set():
            logging.info("Vehicle processing cycle completed, waiting for next cycle")
            await asyncio.sleep(300)  # Wait for 5 minutes before next cycle
        else:
            logging.info("Compression in progress, waiting for it to finish")
            await compression_event.wait()
            logging.info("Compression finished, resuming vehicle processing")

if __name__ == "__main__":
    asyncio.run(main())

