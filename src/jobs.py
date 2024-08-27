import logging
from datetime import datetime, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from s3fs import S3FileSystem
from utils.files import S3FileManager
# import boto3
import pytz
import asyncio
import os
from multiprocessing import Process

from tqdm import tqdm

from bib_models_data_ev.core.config import settings

# #### Every day worker 
# from FleetEvolution.warehouse import InsertWarehouse
# from FleetEvolution.stateevolution import InsertDataState
# from FleetEvolution.modelevolution import InsertDataModel
# from etl.fifteen.fifsac import InfoETLFifsac, EvolutionETLFifsac, ReviewETLFifsac, StateETLFifsac


#### Ponctual worker 

# from etl.tier.tieorg1 import InfoETLTieorg1, DataETLTieorg1, EvolutionETLTieorg1, ReviewETLTieorg1, StateETLTieorg1, DecayedETLTieorg1

from mercedes.mercedes_parsing import mercedes_parsing
# from mercedes.mercedes_raw_ts import mercedes_raw_ts
# from mercedes.mercedes_processed_ts import mercedes_processed_ts


from utils.files import S3FileManager
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


class MercedesTransform(Jobinterval):
    """
    Raw data are collected from the Mercedes API and stored in a json file in the S3 bucket.
    The data are then parsed and transformed into a pandas dataframe.
    The dataframe is then stored in a parquet file in the S3 bucket.
    """
    id: str = "MercedesTransform"
    name: str = "MercedesTransform"
    first_fire_date = datetime.now() + timedelta(seconds=5)
    trigger = IntervalTrigger(days=1, start_date=first_fire_date)
    logger: logging.Logger = logging.getLogger("MercedesTransform")

    now = datetime.now() + timedelta(seconds=5)
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
        fs = S3FileSystem(
            anon=False,
            key=settings.S3_KEY,
            secret=settings.S3_SECRET,
            client_kwargs={"endpoint_url": settings.S3_URL, "region_name": "fr-par"},)
        try:
            date = datetime.now()
            self.logger.info("Parsing data mercedes")
            fm = S3FileManager.from_fs(
                fs,
                settings.S3_BUCKET,
                "response/mercedes-benz",
            )
            for file in fm.list_files():
                self.logger.info(file)
                source = f"{settings.S3_BUCKET}/response/mercedes-benz/{file}"
                dest = f"{settings.S3_BUCKET}/raw_ts/mercedes-benz/{file}"

            ##Parsing response
            await mercedes_parsing(source,dest).run()

            # ##Raw data
            # await mercedes_raw_ts().run()

            # ##Processed data
            # await mercedes_processed_ts().run()

            # ##Parsing response
            # await mercedes_parsing().run()

            # ##Tieorg2
            # source = f"{settings.S3_BUCKET}/tier/ebicycle_tieorg_split.csv"
            # source1 = f"{settings.S3_BUCKET}/tier/escooter_tieorg_split.csv"
            # source2 = f"{settings.S3_BUCKET}/tier/ebicycle_tiesqy_split.csv"  
            # source3 = f"{settings.S3_BUCKET}/tier/escooter_tiesqy_split.csv"
            # source4 = f"{settings.S3_BUCKET}/tier/ebicycle_tiepar_split.csv"
            # sourceX = f"{settings.S3_BUCKET}/tier/merged_new_rental_facts.csv"


            # dest = f"{settings.S3_BUCKET}/tier/old_battery_daily/" + "ebicycle_tieorg"+ str(date) +".csv"
            # dest1 = f"{settings.S3_BUCKET}/tier/old_battery_daily/" + "escooter_tieorg"+ str(date) +".csv"
            # dest2= f"{settings.S3_BUCKET}/tier/old_battery_daily/" + "ebicycle_tiesqy"+ str(date) +".csv"
            # dest3 = f"{settings.S3_BUCKET}/tier/old_battery_daily/" + "escooter_tiesqy"+ str(date) +".csv"
            # dest4 = f"{settings.S3_BUCKET}/tier/old_battery_daily/" + "ebicycle_tiepar"+ str(date) +".csv"

            # destX = f"{settings.S3_BUCKET}/tier/old_rental_facts/merged-" + str(date) +".csv"

            # fs.mv(source, dest)
            # fs.mv(source1, dest1)
            # fs.mv(source2, dest2)
            # fs.mv(source3, dest3)
            # fs.mv(source4, dest4)

            # fs.mv(sourceX, destX)

            # # # await DecayedETLTiepar().run()


 
        except FileNotFoundError as e:
            self.logger.info("No new files")


