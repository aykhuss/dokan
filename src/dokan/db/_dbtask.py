import luigi
import logging
import time
import json

from abc import ABCMeta, abstractmethod
from pathlib import Path
import shutil

from sqlalchemy import create_engine, Engine, select
from sqlalchemy.orm import Session

from ._jobstatus import JobStatus
from ._sqla import JobDB, Part, Job

from ..task import Task
from ..exe import ExecutionMode, ExecutionPolicy, ExeData, Executor
from ..order import Order
from ..runcard import Runcard, RuncardTemplate

logger = logging.getLogger("luigi-interface")


class DBTask(Task, metaclass=ABCMeta):
    """the task class to interact with the database"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # @todo all DBTasks need to be started in the job root path: check?
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

    order: int = luigi.IntParameter(default=Order.NNLO)
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
                for db_pt in session.scalars(stmt):
                    if db_pt.active != Order(db_pt.order).is_in(Order(self.order)):
                        return False

        return True

    def run(self) -> None:
        print(f"DBInit: run {Order(self.order)!r}")
        with self.session as session:
            for db_pt in session.scalars(select(Part)):
                db_pt.active = False  # reset to be safe
            for pt in self.channels:
                stmt = select(Part).where(Part.name == pt)
                # @ todo catch case where it's already there and check it has same entries?
                if not session.scalars(stmt).first():
                    session.add(Part(name=pt, active=False, **self.channels[pt]))
                for db_pt in session.scalars(stmt):
                    db_pt.active = Order(db_pt.order).is_in(Order(self.order))
            session.commit()
        self.print_part()


class DBDispatch(DBTask):
    # > inactive selection: 0
    # > pick a specific `Job` by id: > 0
    # > restrict to specific `Part` by id: < 0 [take abs]
    id: int = luigi.IntParameter(default=0)
    # > mode and policy must be set already before dispatch!

    @property
    def select_job(self):
        # > define the selector for the jobs based on the id that was passed
        if self.id > 0:
            return select(Job).where(Job.id == self.id)
        elif self.id < 0:
            return select(Job).where(Job.part_id == abs(self.id))
        else:
            return select(Job)

    def complete(self) -> bool:
        with self.session as session:
            for job in session.scalars(self.select_job):
                if job.status == JobStatus.QUEUED:
                    return False
            return True

    def run(self):
        print(f"DBDispatch: run {self.id}")
        with self.session as session:
            # > get the front of the queue
            stmt = self.select_job.where(Job.status == JobStatus.QUEUED).order_by(
                Job.id.asc()
            )
            # @todo add batches
            job = session.scalars(stmt).first()
            if job:
                logger.debug(f"{self.id} -> dispatch job:\n>  {job!r}")
                # @todo set seeds here! (batch size rounding and ordering)
                # > get last job that has a seed assigned to it
                last_job = session.scalars(
                    self.select_job.where(Job.mode == job.mode)
                    .where(Job.seed.is_not(None))
                    .order_by(Job.id.desc())
                ).first()
                if last_job:
                    seed_start: int = last_job.seed + 1
                else:
                    seed_start: int = self.config["run"]["seed_offset"]
                job.seed = seed_start
                job.status = JobStatus.DISPATCHED
                session.commit()
                # yield DBRunner(id=job.id)
                yield self.clone(cls=DBRunner, id=job.id)


class DBRunner(DBTask):
    # @todo make a list to accommodate batch jobs
    id: int = luigi.IntParameter()
    # priority = 100

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def complete(self) -> bool:
        with self.session as session:
            job: Job = session.get_one(Job, self.id)
            # @todo add classmethod JobStatus to query properties
            return job.status in [JobStatus.DONE, JobStatus.MERGED, JobStatus.FAILED]

    def get_ntot(self) -> tuple[int, int]:
        """determine statistics to fill `job_max_runtime` using past jobs"""
        # should filder warmup & production separately.
        # for warmup maybe use only last?
        # of generally put in a bias towards newer runs?
        ncall = 100
        niter = 2
        return ncall, niter

    def run(self):
        print(f"DBRunner: run {self.id}")
        with self.session as session:
            db_job: Job = session.get_one(Job, self.id)
            #@todo mode, policy, channel string, etc all should be extracted here for the
            # entire batch before (maybe a dict?)
            # alterantively check for a exe path that is set?
            if db_job.status == JobStatus.DISPATCHED:
                # > assemble job path
                root_path: Path = Path(self.config["run"]["path"])
                job_path: Path = root_path.joinpath(
                    "raw",
                    str(ExecutionMode(db_job.mode)),
                    db_job.part.name,
                    f"s{db_job.seed}",
                )
                # > create a ExeData tmp state and populate
                exe_data = ExeData(job_path)
                exe_data["exe"] = self.config["exe"]["path"]
                exe_data["mode"] = ExecutionMode(db_job.mode)
                exe_data["policy"] = ExecutionPolicy(db_job.policy)
                # @todo: add policy settings
                exe_data["policy_settings"] = {}
                if db_job.policy == ExecutionPolicy.LOCAL:
                    exe_data["policy_settings"]["local_ncores"] = 1
                elif db_job.policy == ExecutionPolicy.HTCONDOR:
                    exe_data["policy_settings"]["htcondor_id"] = 42
                exe_data["ncall"], exe_data["niter"] = self.get_ntot()
                # > create the runcard
                run_file: Path = job_path / "job.run"
                template = RuncardTemplate(
                    Path(self.config["run"]["path"]) / self.config["run"]["template"]
                )
                channel_region:str = ""
                if db_job.part.region:
                    channel_region:str = f"region = {db_job.part.region}"
                template.fill(
                    run_file,
                    sweep=f"{exe_data["mode"]!s} = {exe_data["ncall"]}[{exe_data["niter"]}]",
                    run="",
                    channels=db_job.part.string,
                    channels_region=channel_region,
                    toplevel="",
                )
                exe_data["input_files"] = ["job.run"]  # ensure it's always at the front
                # > get last warmup to copy grid files
                last_warm = session.scalars(
                    select(Job)
                    .where(Job.part_id == db_job.part_id)
                    .where(Job.mode == ExecutionMode.WARMUP)
                    .where(Job.status == JobStatus.DONE)
                    .order_by(Job.id.desc())
                ).first()
                if not last_warm and db_job.mode == ExecutionMode.PRODUCTION:
                    raise RuntimeError(
                        f"no warmup found for production job {db_job.part.name}"
                    )
                if last_warm:
                    if not last_warm.path:
                        raise RuntimeError(f"last warmup {last_warm.id} has no path")
                    last_warm_path: Path = Path(last_warm.path)
                    last_warm_data: ExeData = ExeData(last_warm_path)
                    if not last_warm_data.is_final:
                        raise RuntimeError(f"last warmup {last_warm.id} is not final")
                    for wfile in last_warm_data["output_files"]:
                        shutil.copyfile(last_warm_path / wfile, job_path / wfile)
                        exe_data["input_files"].append(wfile)
                # @ todo FIRST put the runcard
                exe_data["jobs"] = {}
                exe_data["jobs"][db_job.id] = {
                    "seed": db_job.seed,
                }
                # @todo: better to do this in the executor run to alighn when job was actually started?
                exe_data["timestamp"] = time.time()
                # save to tmp file
                exe_data.write()
                # > commit update
                db_job.path = str(job_path)
                db_job.status = JobStatus.RUNNING
                session.commit()
            yield Executor.factory(
                policy=ExecutionPolicy(db_job.policy), path=db_job.path
            )
            # @todo parse the return
            exe_data = ExeData(db_job.path)
            if not exe_data.is_final:
                raise RuntimeError(f"{db_job.id} is not final?!")

        # logger.debug(f"{self.id}: collected heavy")
        # # > save result of heavy:
        # with heavy.open("r") as heavy_fh:
        #     res = json.load(heavy_fh)
        #     if "val" in res:
        #         with self.session as session:
        #             job: Job = session.get_one(Job, self.id)
        #             logger.debug(
        #                 f"{self.id}: job: {job!r} gave back val = {res["val"]}"
        #             )
        #             job.result = res["val"]
        #             job.status = JobStatus.DONE
        #             session.commit()


#    def copy_input(self):
#        if not self.input_local_path:
#            return
#
#        def input(*path: PathLike) -> Path:
#            return Path(self.config["run"]["path"]).joinpath(
#                *self.input_local_path, *path
#            )
#
#        with open(input(Executor._file_res), "r") as in_res:
#            in_data = json.load(in_res)
#            # > check here if it's a warmup?
#            for in_file in in_data["output_files"]:
#                # > always skip log (& dat) files
#                # > if warmup copy over also txt files
#                # > for production, only take the weights (skip txt)
#                if in_file in self.data["input_files"]:
#                    continue  # already copied in the past
#                shutil.copyfile(input(in_file), self._local(in_file))
#                self.data["input_files"].append(in_file)
#

#    def write_runcard(self):
#        runcard = self._local(Executor._file_run)
#        # > tempalte variables: {sweep, run, channels, channels_region, toplevel}
#        channel_string = self.config["process"]["channels"][self.channel]["string"]
#        if "region" in self.config["process"]["channels"][self.channel]:
#            channel_region = self.config["process"]["channels"][self.channel]["region"]
#        else:
#            channel_region = ""
#        dokan.runcard.fill_template(
#            runcard,
#            self.config["run"]["template"],
#            sweep=f"{self.mode!s} = {self.ncall}[{self.niter}]",
#            run="",
#            channels=channel_string,
#            channels_region=channel_region,
#            toplevel="",
#        )
#        self.data["input_files"].append(Executor._file_run)

#        runcard = self._local(Executor._file_run)
#        # > tempalte variables: {sweep, run, channels, channels_region, toplevel}
#        channel_string = self.config["process"]["channels"][self.channel]["string"]
#        if "region" in self.config["process"]["channels"][self.channel]:
#            channel_region = self.config["process"]["channels"][self.channel]["region"]
#        else:
#            channel_region = ""
#        dokan.runcard.fill_template(
#            runcard,
#            self.config["run"]["template"],
#            sweep="{} = {}[{}]".format(
#                ExecutionMode(self.exe_mode), self.ncall, self.niter
#            ),
#            run="",
#            channels=channel_string,
#            channels_region=channel_region,
#            toplevel="",
#        )
