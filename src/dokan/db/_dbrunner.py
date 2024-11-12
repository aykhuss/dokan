import luigi
import logging
import time
import json
import re
import shutil
import math
import datetime

from abc import ABCMeta, abstractmethod
from pathlib import Path

from sqlalchemy import create_engine, Engine, select, func
from sqlalchemy.orm import Session  # , scoped_session, sessionmaker

from rich.console import Console

from ._dbtask import DBTask
from ._dbmerge import MergePart
from ._jobstatus import JobStatus
from ._loglevel import LogLevel
from ._sqla import DokanDB, Part, Job, Log

from ..task import Task
from ..exe import ExecutionMode, ExecutionPolicy, ExeData, Executor
from ..order import Order
from ..runcard import Runcard, RuncardTemplate


class DBRunner(DBTask):
    # @todo make a list to accommodate batch jobs
    id: int = luigi.IntParameter()
    # priority = 100

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # @todo find the part id & check they are the same for all in the batch
        # @todo check that the seeds are sequential
        # > preliminarily implement part_id
        with self.session as session:
            job: Job = session.get_one(Job, self.id)
            self.part_id: int = job.part_id
            self.mode: ExecutionMode = ExecutionMode(job.mode)

    def complete(self) -> bool:
        with self.session as session:
            job: Job = session.get_one(Job, self.id)
            # @todo add classmethod JobStatus to query properties
            return job.status in JobStatus.terminated_list()

    def run(self):
        self.logger(f"DBRunner::run id = {self.id}, part_id = {self.part_id}")
        with self.session as session:
            db_job: Job = session.get_one(Job, self.id)
            # @todo mode, policy, channel string, etc all should be extracted here for the
            # entire batch before (maybe a dict?)
            # alterantively check for a exe path that is set?
            if db_job.status == JobStatus.DISPATCHED:
                # > assemble job path
                job_path: Path = self._path.joinpath(
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
                # > add policy settings
                exe_data["policy_settings"] = {
                    "max_runtime": self.config["run"]["job_max_runtime"]
                }
                # if db_job.policy == ExecutionPolicy.LOCAL:
                #     exe_data["policy_settings"]["local_ncores"] = 1
                # elif db_job.policy == ExecutionPolicy.HTCONDOR:
                #     exe_data["policy_settings"]["htcondor_id"] = 42
                for k,v in self.config["exe"]["policy_settings"].items():
                    exe_data["policy_settings"][k] = v
                # exe_data["policy_settings"] = self.config["exe"]["policy_settings"]
                if (db_job.ncall * db_job.niter) == 0:
                    raise RuntimeError(f"job {db_job.id} has ntot={db_job.ncall}×{db_job.niter}=0")
                exe_data["ncall"] = db_job.ncall
                exe_data["niter"] = db_job.niter
                # > create the runcard
                run_file: Path = job_path / "job.run"
                template = RuncardTemplate(
                    Path(self.config["run"]["path"]) / self.config["run"]["template"]
                )
                channel_region: str = ""
                if db_job.part.region:
                    channel_region: str = f"region = {db_job.part.region}"
                template.fill(
                    run_file,
                    sweep=f"{exe_data['mode']!s} = {exe_data['ncall']}[{exe_data['niter']}]",
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
                    raise RuntimeError(f"no warmup found for production job {db_job.part.name}")

                if last_warm:
                    if not last_warm.rel_path:
                        raise RuntimeError(f"last warmup {last_warm.id} has no path")
                    last_warm_path: Path = self._path / last_warm.rel_path
                    last_warm_data: ExeData = ExeData(last_warm_path)
                    if not last_warm_data.is_final:
                        raise RuntimeError(f"last warmup {last_warm.id} is not final")
                    for wfile in last_warm_data["output_files"]:
                        # @todo always skip log (& dat) files
                        # @todo if warmup copy over also txt files
                        # @todo for production, only take the weights (skip txt)
                        # > skip "*.s<seed>.*" files & job files
                        if re.match(r"^.*\.s[0-9]+\.[^0-9.]+$", wfile):
                            continue
                        if re.match(r"^job.*$", wfile):
                            continue
                        shutil.copyfile(last_warm_path / wfile, job_path / wfile)
                        exe_data["input_files"].append(wfile)
                # @ todo FIRST put the runcard
                exe_data["jobs"] = {}
                exe_data["jobs"][db_job.id] = {
                    "seed": db_job.seed,
                }
                exe_data["output_files"] = []
                # save to tmp file (this also updates the timestamp!)
                exe_data.write()
                # > commit update
                db_job.rel_path = str(job_path.relative_to(self._path))
                db_job.status = JobStatus.RUNNING
                session.commit()
            # self.logger(f"DBRunner[{self.id}]: checking {db_job.rel_path}")
            yield Executor.factory(
                policy=ExecutionPolicy(db_job.policy), path=str(self._path / db_job.rel_path)
            )
            # > parse the retun data
            exe_data = ExeData(self._path / db_job.rel_path)
            if not exe_data.is_final:
                raise RuntimeError(f"{db_job.id} is not final?!\n{exe_data.path}\n{exe_data.data}")
            if "result" in exe_data["jobs"][db_job.id]:
                db_job.result = exe_data["jobs"][db_job.id]["result"]
                db_job.error = exe_data["jobs"][db_job.id]["error"]
                db_job.chi2dof = exe_data["jobs"][db_job.id]["chi2dof"]
                db_job.elapsed_time = exe_data["jobs"][db_job.id]["elapsed_time"]
                db_job.status = JobStatus.DONE
            else:
                db_job.status = JobStatus.FAILED
            session.commit()

        # > see if a re-merge is possible
        if self.mode == ExecutionMode.PRODUCTION:
            mrg_part: MergePart = self.clone(MergePart, force=False, part_id=self.part_id)
            if mrg_part.complete():
                self.logger(f"DBRunner::run id = {self.id}, part_id = {self.part_id} > MergePart complete")
                return
            else:
                self.logger(f"DBRunner::run id = {self.id}, part_id = {self.part_id} > yield MergePart")
                yield mrg_part
