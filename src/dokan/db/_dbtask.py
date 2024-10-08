import luigi
import logging

from abc import ABCMeta, abstractmethod


from sqlalchemy import create_engine, Engine, select
from sqlalchemy.orm import Session

from ._jobstatus import JobStatus
from ._sqla import JobDB, Part, Job
from ..task import Task

logger = logging.getLogger("luigi-interface")


class DBTask(Task, metaclass=ABCMeta):
    """the task class to interact with the database"""

    # @todo: add database name as luigi parameter

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dbname: str = "sqlite:///" + str(self._local("db.sqlite"))

    @property
    def engine(self) -> Engine:
        return create_engine(self.dbname)

    @property
    def session(self) -> Session:
        return Session(self.engine)

    def print_part(self) -> None:
        with self.session as session:
            for pt in session.scalars(select(Part)):
                print(pt)

    def print_job(self) -> None:
        with self.session as session:
            for job in session.scalars(select(Job)):
                print(job)

    # > database queries should jump the scheduler queue
    # > threadsafety using resource = 1, where read/write needed
    resources = {"DBTask": 1}
    # priority = 100

    def output(self):
        # DBHandlers do not have output files but use the DB
        return []

    @abstractmethod
    def complete(self) -> bool:
        return False


class DBInit(DBTask):
    """initilization of the 'parts' table of the database with process channel information"""

    channels: dict = luigi.DictParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        JobDB.metadata.create_all(self.engine)

    def complete(self) -> bool:
        with self.session as session:
            for pt in self.channels:
                stmt = select(Part).where(Part.name == pt)
                # stmt = select(Part).where(Part.name == pt).exists()
                if not session.scalars(stmt).first():
                    return False
        return True

    def run(self) -> None:
        with self.session as session:
            for pt in self.channels:
                # catch case where it's already there and check it has same entries?
                session.add(Part(name=pt, **self.channels[pt]))
            session.commit()
        self.print_part()
        self.print_job()


# class DBDispatch(DBTask):
#
#     #> inactive selection: 0
#     #> pick a specific `Job` by id: > 0
#     #> restrict to specific `Part` by id: < 0 [take abs]
#     id: int = luigi.IntParameter(default=0, significant=False)
#
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         # # > queue up the jobs
#         # with self.session as session:
#         #     pt: Part = Part(name="dis" + self.jobs[0])
#         #     for job_name in self.jobs:
#         #         session.add(Job(name=job_name, status=JobStatus.QUEUED, part=pt))
#         #     session.commit()
#
#     def complete(self) -> bool:
#         with self.session as session:
#             stmt = select(Job)
#             if self.id > 0:
#                 stmt = select(Job).where(Job.id == self.id)
#             elif self.id < 0:
#                 stmt = select(Job).where(Job.part_id == abs(self.id))
#             for job in session.scalars(stmt):
#                 if job.status == JobStatus.QUEUED:
#                     return False
#             return True
#
#     def run(self) -> None:
#         with self.session as session:
#             stmt = select(Job).where(Job.status == JobStatus.QUEUED).order_by(Job.id)
#             job = session.scalars(stmt).first()
#             if job:
#                 logger.debug(f"{self.name} -> dispatch job: {job}")
#                 job.status = JobStatus.DISPATCHED
#                 session.commit()
#                 yield DBRunner(id=job.id)


## class DBRunner(DBHandler):
##     id: int = luigi.IntParameter()
##     # priority = 100
##
##     def __init__(self, *args, **kwargs):
##         super().__init__(*args, **kwargs)
##
##     def complete(self) -> bool:
##         with self.session as session:
##             job: Job = session.get_one(Job, self.id)
##             return job.status in [JobStatus.DONE, JobStatus.MERGED, JobStatus.FAILED]
##
##     def run(self) -> None:
##         with self.session as session:
##             job: Job = session.get_one(Job, self.id)
##             job.status = JobStatus.RUNNING
##             session.commit()
##             heavy = yield Heavy(name=job.name, seed=job.id)
##
##         logger.debug(f"{self.id}: collected heavy")
##         # > save result of heavy:
##         with heavy.open("r") as heavy_fh:
##             res = json.load(heavy_fh)
##             if "val" in res:
##                 with self.session as session:
##                     job: Job = session.get_one(Job, self.id)
##                     logger.debug(
##                         f"{self.id}: job: {job!r} gave back val = {res["val"]}"
##                     )
##                     job.result = res["val"]
##                     job.status = JobStatus.DONE
##                     session.commit()
##
##
