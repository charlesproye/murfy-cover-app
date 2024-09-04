import logging
from datetime import datetime, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from s3fs import S3FileSystem
# from utils.files import S3FileManager
import boto3
import pytz
import asyncio
import os
from multiprocessing import Process
import pandas as pd
from tqdm import tqdm

from bib_models_data_ev.core.config import settings


#### Ponctual worker 

from mercedes.mercedes_parsing import mercedes_parsing
from mercedes.mercedes_raw_ts import mercedes_raw_ts
# from mercedes.mercedes_processed_ts import mercedes_processed_ts


# from utils.files import S3FileManager
from utils.format import log_process



####################################################### Defining the class Job #######################################################################
######################################################################################################################################################
class Jobtime: ### job defined that will run at a specific time  
    name: str
    file : str
    path_source : str
    path_dest : str
    trigger: IntervalTrigger
    logger: logging.Logger
    max_running_jobs: int = 1
    max_event_size: int = 1

    def run_process(self, job_func, job_obj=None):
        # We create a new process to run the job
        p = Process(target=self.run_async_job, args=(job_func,job_obj))
        p.start()
        p.join()

    def run_async_job(self, job_func, job_obj=None):
        self.logger.info(f"Running job on {os.getpid()}")
        # that run it's proper async loop
        asyncio.run(job_func(job_obj))

    async def add_to_schedule(self, scheduler: AsyncIOScheduler):
        job = scheduler.add_job(
            self.run_process,
            args=(self.func,),
            trigger=self.trigger,
            name=self.name,
            id=self.name,
            max_instances=self.max_running_jobs,
            coalesce=True,
            replace_existing=True,
            misfire_grace_time=None,
        )
        log_process(self.logger, self.trigger)
        job.kwargs["job_obj"] = job
        return job

    @classmethod
    async def func(cls, job_obj=None) :
        fs = S3FileSystem(
            anon=False,
            key=settings.S3_KEY,
            secret=settings.S3_SECRET,
            client_kwargs={"endpoint_url": settings.S3_URL, "region_name": "fr-par"},
        )
        try:
            cls.logger.info(job_obj.name + "modified since last run, running ETL...")
            etl = cls.function()
            await etl.run()
            # fs.mv(source, dest)
        except FileNotFoundError: # Should not happen since the insertion of the file is no moere a trigger
            cls.logger.info("No new files")


class Jobinterval: ###Job for which we define an interval between 2 launch ### Still not generalised between all the company
    name: str
    file : str
    path_source : str
    path_dest : str
    trigger: IntervalTrigger
    logger: logging.Logger
    max_running_jobs: int = 1
    max_event_size: int = 1

    def run_process(self, job_func, job_obj=None):
        p = Process(target=self.run_async_job, args=(job_func,job_obj))
        p.start()
        p.join()

    def run_async_job(self, job_func, job_obj=None):
        self.logger.info(f"Running job on {os.getpid()}")
        asyncio.run(job_func(job_obj))

    async def add_to_schedule(self, scheduler: AsyncIOScheduler):
        job = scheduler.add_job(
            self.run_process,
            args=(self.func,),
            trigger=self.trigger,
            name=self.name,
            id = self.name,
            max_instances=self.max_running_jobs,
            coalesce=True,
            replace_existing=True,
            misfire_grace_time=None,
        )
        log_process(self.logger, self.trigger)
        job.kwargs["job_obj"] = job
        return job

    @classmethod
    async def func(cls, job_obj=None, function : callable=None ) :
        pass


######################################################################## Worker working 1 time everyweek #################################################
########################################################################################################################################################

# class ProdToDev(Jobtime):
#     name: str = "ProdToDev"
#     logger: logging.Logger = logging.getLogger("ProdToDev")
#     # lancement uniquement le dimanche à 12h
#     trigger = CronTrigger(hour=12, day_of_week=6)
#     # Pour lancer après 5 secondes
#     now=datetime.now()+timedelta(seconds=5)
#     hour,minute,second=now.hour,now.minute,now.second
#     trigger = CronTrigger(hour= hour, minute=minute, second=second)


#     function: callable  = InsertProdToDev

###################################################################### Upload of new data ###########################################################################
#####################################################################################################################################################################

# class RepairCitpar2Upload(Jobinterval):
#     id : str = "RepairCityscoot"
#     logger: logging.Logger = logging.getLogger("RepairCityscoot")
#     trigger = IntervalTrigger(seconds=10)
#     filename = "repair_cityscoot7.xlsx"

#     now = datetime.now() + timedelta(seconds=5)
#     hour,minute,second=now.hour,now.minute,now.second
#     trigger = CronTrigger(hour= hour, minute=minute, second=second)

#     @classmethod
#     async def func(cls, job_obj=None):
#         fs = S3FileSystem(
#             anon=False,
#             key=settings.S3_KEY,
#             secret=settings.S3_SECRET,
#             client_kwargs={"endpoint_url": settings.S3_URL, "region_name": "fr-par"},)
#         try:
#             date = datetime.now()
#             source = f"{settings.S3_BUCKET}/citpar/easyli/" + cls.filename
#             dest = f"{settings.S3_BUCKET}/citpar/easyli_old/cityscoot-" + str(date) +".xlsx"
#             last_edit_datetime = fs.info(source)["LastModified"]
#             last_edit_datetime = last_edit_datetime.astimezone(pytz.UTC)
#             cls.logger.info("Cityscoot Upload modified since last run, running ETL...")
#             fm = S3FileManager.from_fs(
#                 fs,
#                 settings.S3_BUCKET,
#                 "citpar/easyli",
#             )
#             await RepairETLCit2(fm, cls.filename).run()

#             fs.mv(source, dest)
#             fs.invalidate_cache()
#         except FileNotFoundError as e:
#             cls.logger.info("No new files")


class MercedesParsing(Jobinterval):
    """
    Raw data are collected from the Mercedes API and stored in a json file in the S3 bucket.
    The data are then parsed and transformed into a pandas dataframe.
    The dataframe is then stored in a parquet file in the S3 bucket.
    """
    id: str = "MercedesTransform"
    name: str = "MercedesTransform"
    first_fire_date = datetime.now() + timedelta(seconds=2)
    trigger = IntervalTrigger(days=1, start_date=first_fire_date)
    logger: logging.Logger = logging.getLogger("MercedesTransform")

    now = datetime.now() + timedelta(seconds=2)
    trigger = CronTrigger(
        year=now.year,
        month=now.month,
        day=now.day,
        hour=now.hour,
        minute=now.minute,
        second=now.second,
    )

    @classmethod
    async def func(self, job_obj=None):
        ##############using boto3 ##################
        session = boto3.Session(
                aws_access_key_id=settings.S3_KEY,  # S3_KEY de Scaleway
                aws_secret_access_key=settings.S3_SECRET,  # S3_SECRET de Scaleway
                region_name='fr-par'  # Région Scaleway
        )
        s3 = session.client(
                's3',
                endpoint_url=settings.S3_URL  # Point de terminaison Scaleway
            )
        response = s3.list_objects_v2(Bucket=settings.S3_BUCKET, Prefix="response/mercedes-benz/", Delimiter="/")
        folders = [prefix['Prefix'] for prefix in response['CommonPrefixes']]
        for folder_path in folders[2:4]: # The two first one are juste the same root as the folder
            files_response = s3.list_objects_v2(Bucket=settings.S3_BUCKET, Prefix=folder_path)
            files = [item['Key'] for item in files_response.get('Contents', []) if item['Key'].lower().endswith('.json')]
            for file_path in files[2:4]: 
                file = os.path.basename(file_path)
                source = f"{folder_path}{file}"
                dest_response = source.replace('response/', 'raw_ts/').replace('.json', '.csv')
                dest_raw_ts = source.replace('response/', 'raw_ts/').replace('.json', '.csv')
                dest_processed_ts = source.replace('response/', 'processed_ts/').replace('.json', '.csv')
                try:
                    await mercedes_parsing(s3, source, dest_response).run()
                except Exception as e:
                    print(f"Error loading file: {e}")
        ###############using s3fs ##################
        # fs = S3FileSystem(
        #     anon=False,
        #     key=settings.S3_KEY,
        #     secret=settings.S3_SECRET,
        #     client_kwargs={"endpoint_url": settings.S3_URL, "region_name": "fr-par"},)
        # try:
        #     date = datetime.now()
        #     self.logger.info("Parsing data mercedes")
        #     fm1 = S3FileManager.from_fs(
        #         fs,
        #         settings.S3_BUCKET,
        #         "response/mercedes-benz/",
        #     )
        #     fm2 = S3FileManager.from_fs(
        #         fs,
        #         settings.S3_BUCKET,
        #         "raw_ts/mercedes-benz/",
        #     )
        #     # Test S3FileManager separately
        #     # test_source = "2024-08-14.json"
        #     # try:
        #     #     test_content = fm1.load(test_source)
        #     #     print(f"Test file content loaded, length: {len(test_content)}")
        #     # except Exception as e:
        #     #     print(f"Error loading test file: {type(e).__name__}: {str(e)}")
        #     folders = fs.ls(f"{settings.S3_BUCKET}/response/mercedes-benz/", detail=False)
        #     for folder_path in folders[2:4]: # The two first one are juste the same root as the folder
        #         files = fs.ls(folder_path, detail=False)
        #         folder = os.path.basename(folder_path)
        #         for file_path in files[2:4]: 
        #             file = os.path.basename(file_path)
        #             if file.lower().endswith('.json'):
        #                 self.logger.info(f"Processing file: {file}")
        #                 source = f"{folder}/{file}"
        #                 dest = f"{folder}/{file}"
        #                 dest= dest.replace(".json", ".csv")
        #                 print("type(dest)", type(dest))
        #                 print(f"Source: {source}")
        #                 print(f"Destination: {dest}")
        #                 # fm2.save(fs, pd.DataFrame(), "")  # Correct folder creation
        #                 # # if not fs.exists(full_s3_path):
        #                 #     # print("Folder doesn't exists !!!")
        #                 #     # fs.makedirs(full_s3_path, exist_ok=True)
        #                 #     # fs.touch("bib-platform-dev-data/raw_ts/mercedes-benz/kiwi")
        #                 #     # pd.DataFrame().to_csv("bib-platform-dev-data/raw_ts/mercedes-benz/kiwi/kiwi.csv")
        #                 #     # print(f'Folder created')
        #                 try:
        #                     await mercedes_parsing(fs, fm1, fm2, source, dest).run()
        #                 except Exception as e:
        #                     print(f"Error loading file: {e}")

                    # ##Raw data
                    # await mercedes_raw_ts().run()

                    # ##Processed data
                    # await mercedes_processed_ts().run()

 
        # except FileNotFoundError as e:
        #     self.logger.info("No new files")

class MercedesRawTs(Jobinterval):

    """
    Raw data are collected from the Mercedes API and stored in a json file in the S3 bucket.
    The data are then parsed and transformed into a pandas dataframe.
    The dataframe is then stored in a parquet file in the S3 bucket.
    """
    id: str = "MercedesRawTs"
    name: str = "MercedesRawTs"
    first_fire_date = datetime.now() + timedelta(seconds=2)
    trigger = IntervalTrigger(days=1, start_date=first_fire_date)
    logger: logging.Logger = logging.getLogger("MercedesRawTs")

    now = datetime.now() + timedelta(seconds=2)
    trigger = CronTrigger(
        year=now.year,
        month=now.month,
        day=now.day,
        hour=now.hour,
        minute=now.minute,
        second=now.second,
    )

    @classmethod
    async def func(self, job_obj=None):
        ##############using boto3 ##################
        session = boto3.Session(
                aws_access_key_id=settings.S3_KEY,  # S3_KEY de Scaleway
                aws_secret_access_key=settings.S3_SECRET,  # S3_SECRET de Scaleway
                region_name='fr-par'  # Région Scaleway
        )
        s3 = session.client(
                's3',
                endpoint_url=settings.S3_URL  # Point de terminaison Scaleway
            )
        response = s3.list_objects_v2(Bucket=settings.S3_BUCKET, Prefix="raw_ts/mercedes-benz/", Delimiter="/")
        folders = [prefix['Prefix'] for prefix in response['CommonPrefixes']]
        for folder_path in folders[2:4]: # The two first one are juste the same root as the folder
            files_response = s3.list_objects_v2(Bucket=settings.S3_BUCKET, Prefix=folder_path)
            files = [item['Key'] for item in files_response.get('Contents', []) if item['Key'].lower().endswith('.json')]
            for file_path in files[2:4]: 
                file = os.path.basename(file_path)
                source = f"{folder_path}{file}"
                dest_response = source.replace('raw_ts/', 'processed_ts/').replace('.csv', '.parquet')
                dest_raw_ts = source.replace('raw_ts/', 'processed_ts/').replace('.csv', '.parquet')
                dest_processed_ts = source.replace('raw_ts/', 'processed_ts/').replace('.csv', '.parquet')
                try:
                    await mercedes_raw_ts(s3, source, dest_response).run()
                except Exception as e:
                    print(f"Error loading file: {e}")

class MercedesProcessedTs(Jobinterval):
    
    """
    Raw data are collected from the Mercedes API and stored in a json file in the S3 bucket.
    The data are then parsed and transformed into a pandas dataframe.
    The dataframe is then stored in a parquet file in the S3 bucket.
    """
    id: str = "MercedesTransform"
    name: str = "MercedesTransform"
    first_fire_date = datetime.now() + timedelta(seconds=2)
    trigger = IntervalTrigger(days=1, start_date=first_fire_date)
    logger: logging.Logger = logging.getLogger("MercedesTransform")

    now = datetime.now() + timedelta(seconds=2)
    trigger = CronTrigger(
        year=now.year,
        month=now.month,
        day=now.day,
        hour=now.hour,
        minute=now.minute,
        second=now.second,
    )

    @classmethod
    async def func(self, job_obj=None):
        ##############using boto3 ##################
        session = boto3.Session(
                aws_access_key_id=settings.S3_KEY,  # S3_KEY de Scaleway
                aws_secret_access_key=settings.S3_SECRET,  # S3_SECRET de Scaleway
                region_name='fr-par'  # Région Scaleway
        )
        s3 = session.client(
                's3',
                endpoint_url=settings.S3_URL  # Point de terminaison Scaleway
            )
        response = s3.list_objects_v2(Bucket=settings.S3_BUCKET, Prefix="raw_ts/mercedes-benz/", Delimiter="/")
        folders = [prefix['Prefix'] for prefix in response['CommonPrefixes']]
        for folder_path in folders[2:4]: # The two first one are juste the same root as the folder
            files_response = s3.list_objects_v2(Bucket=settings.S3_BUCKET, Prefix=folder_path)
            files = [item['Key'] for item in files_response.get('Contents', []) if item['Key'].lower().endswith('.json')]
            for file_path in files[2:4]: 
                file = os.path.basename(file_path)
                source = f"{folder_path}{file}"
                dest_response = source.replace('raw_ts/', 'processed_ts/').replace('.csv', '.parquet')
                dest_raw_ts = source.replace('raw_ts/', 'processed_ts/').replace('.csv', '.parquet')
                dest_processed_ts = source.replace('raw_ts/', 'processed_ts/').replace('.csv', '.parquet')
                try:
                    await mercedes_parsing(s3, source, dest_response).run()
                except Exception as e:
                    print(f"Error loading file: {e}")
