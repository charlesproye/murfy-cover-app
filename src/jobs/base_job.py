import logging
from abc import ABC, abstractmethod

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import asyncio
import os
from multiprocessing import Process

#### Ponctual worker 
from utils.format import log_process


class Jobinterval(ABC): ###Job for which we define an interval between 2 launch ### Still not generalised between all the company
    """
    ### Description:
    Abstract base class for jobs that run on fixed intervals. 
    """
    name: str
    trigger: IntervalTrigger
    logger: logging.Logger
    max_running_jobs: int = 1
    max_event_size: int = 1

    async def add_to_schedule(self, scheduler: AsyncIOScheduler):
        job = scheduler.add_job(
            self.run_in_child_process,
            trigger=self.trigger,
            name=self.name,
            id = self.name,
            max_instances=self.max_running_jobs,
            coalesce=True,
            replace_existing=True,
            misfire_grace_time=None,
        )
        log_process(self.logger, self.trigger)
        
        return job

    def run_in_child_process(self):
        p = Process(target=self.run_async_job)
        p.start()
        p.join()


    def run_async_job(self):
        self.logger.info(f"Running job on {os.getpid()}")
        asyncio.run(self.func())

    @abstractmethod
    async def func(self) :
        pass


