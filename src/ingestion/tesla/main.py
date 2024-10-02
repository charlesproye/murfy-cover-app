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
from datetime import datetime, time as dt_time

# Global variables to store account information
accounts_info = []
compression_event = asyncio.Event()

async def main():
    setup_logging()
    load_dotenv()

    parser = argparse.ArgumentParser(description="Fetch and save Tesla vehicle data.")
    parser.add_argument("--accounts", required=True, type=str, help="JSON string containing account information")
    args = parser.parse_args()

    global accounts_info
    accounts_info = json.loads(args.accounts)

    compression_queue = asyncio.Queue()
    compression_task = asyncio.create_task(schedule_compression(compression_queue))

    if os.getenv("TESLA_COMPRESS") == "1":
        logging.info("Immediate compression requested")
        await compression_queue.put(True)  # Request immediate compression
        await compression_task  # Wait for the immediate compression to complete

    vehicle_tasks = [process_vehicle(account) for account in accounts_info]
    
    await asyncio.gather(compression_task, *vehicle_tasks)

async def process_vehicle(account):
    access_token_key = account['access_token_key']
    refresh_token_key = account['refresh_token_key']
    professional_account = account.get('professional_account', False)
    vehicle_id = account.get('vehicle_id')

    if professional_account:
        vehicle_ids = await fetch_all_vehicle_ids(access_token_key, refresh_token_key)
    else:
        vehicle_ids = [vehicle_id]

    while True:
        for vid in vehicle_ids:
            await compression_event.wait()  # Attendre si une compression est en cours
            try:
                await job(vid, access_token_key, refresh_token_key)
            except Exception as e:
                logging.error(f"Error processing vehicle {vid}: {str(e)}")
            
            if not compression_event.is_set():
                break 
        
        # Attendre 4 minutes avant le prochain cycle, mais vérifier l'événement toutes les 10 secondes
        for _ in range(24):  # 24 * 10 secondes = 4 minutes
            await asyncio.sleep(10)
            if not compression_event.is_set():
                break  # Sortir de la boucle d'attente si une compression a commencé


async def schedule_compression(compression_queue):
    global compression_event
    compression_event = asyncio.Event()
    compression_event.set()  # Initially allow vehicle processing

    while True:
        try:
            immediate = await asyncio.wait_for(compression_queue.get(), timeout=1)
        except asyncio.TimeoutError:
            immediate = False

        if immediate:
            logging.info("Performing immediate compression")
            compression_event.clear()
            await compression_task()
            compression_event.set()
        else:
            now = datetime.now()
            midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
            if now.time() > dt_time(0, 0):
                midnight = midnight.replace(day=now.day + 1)
            
            seconds_until_midnight = (midnight - now).total_seconds()
            await asyncio.sleep(seconds_until_midnight)

            compression_event.clear()  # Stop vehicle processing
            logging.info("Starting scheduled compression task")
            await compression_task()
            logging.info("Scheduled compression task completed")
            compression_event.set()  # Resume vehicle processing

        await asyncio.sleep(1)

async def compression_task():
    logging.info("Starting compression task")
    try:
        await compress_data()
        logging.info("Compression completed successfully")
    except Exception as e:
        logging.error(f"Error during compression: {str(e)}")
    finally:
        logging.info("Compression task finished")

if __name__ == "__main__":
    asyncio.run(main())
