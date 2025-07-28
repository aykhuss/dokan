"""dokan job runner

defines the task to run NNLOJET jobs by spawning executors of the appropriate
backend as specified by the job policy. It further is responsible to populate
the database with the results of each execution (as executors are normal luigi
tasks and, as such, do not have access to the dokan job database).
"""

import math
import re
import shutil
from pathlib import Path

import luigi
from sqlalchemy import select

from ..exe import ExecutionMode, ExecutionPolicy, Executor, ExeData
from ..runcard import RuncardTemplate
from ._dbmerge import MergePart
from ._dbtask import DBTask
from ._jobstatus import JobStatus
from ._loglevel import LogLevel
from ._sqla import Job, Part


class DBRunner(DBTask):
    _file_run: str = "job.run"

    ids: list[int] = luigi.ListParameter()
    part_id: int = luigi.IntParameter()

    priority = 10

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._logger_prefix: str = self.__class__.__name__

        with self.session as session:
            pt: Part = session.get_one(Part, self.part_id)
            self._logger_prefix = self.__class__.__name__ + f"[{pt.name}]"
            self._debug(session, self._logger_prefix + "::init")

            jobs: list[Job] = list(session.scalars(select(Job).where(Job.id.in_(self.ids))).all())
            assert all(j.part_id == self.part_id for j in jobs)
            self.mode: ExecutionMode = ExecutionMode(jobs[0].mode)
            assert all(j.mode == self.mode for j in jobs)
            self.policy: ExecutionPolicy = ExecutionPolicy(jobs[0].policy)
            assert all(j.policy == self.policy for j in jobs)
            # > sequential seed range
            seeds: list[int] = sorted(j.seed for j in jobs if j.seed is not None)
            assert len(seeds) == len(jobs)
            min_seed: int = min(seeds)
            max_seed: int = max(seeds)
            assert len(jobs) == (max_seed - min_seed + 1)
            # > assemble job path
            self.part_name: str = jobs[0].part.name
            self.job_path: Path = self._path.joinpath(
                "raw",
                str(self.mode),
                self.part_name,
                (f"s{min_seed}" if min_seed == max_seed else f"s{min_seed}-{max_seed}"),
            )
            # > same dispatch -> same statistics
            self.ncall: int = jobs[0].ncall
            self.niter: int = jobs[0].niter
            assert all(j.ncall == self.ncall for j in jobs)
            assert all(j.niter == self.niter for j in jobs)
            if (self.niter * self.ncall) == 0:
                raise RuntimeError(f"job {jobs[0].id} has ntot={self.ncall}×{self.niter}==0")

    def complete(self) -> bool:
        with self.session as session:
            for job_id in self.ids:
                if session.get_one(Job, job_id).status not in JobStatus.terminated_list():
                    return False
        return True

    def run(self):
        exe_data = ExeData(self.job_path)

        with self.session as session:
            self._logger(session, self._logger_prefix + f"::run:  [dim](job_ids = {self.ids})[/dim]")

            # > DBDispatch takes care to stay within batch size
            db_jobs: list[Job] = []
            for job_id in self.ids:
                db_jobs.append(session.get_one(Job, job_id))

            job_status: JobStatus = JobStatus(db_jobs[0].status)
            if job_status in JobStatus.active_list():
                assert all(j.status == job_status for j in db_jobs)

            if job_status == JobStatus.DISPATCHED and not exe_data.is_final:
                self._debug(session, self._logger_prefix + "::run:  prepare execution")
                # > populate ExeData with all necessary information for the Executor
                exe_data["exe"] = self.config["exe"]["path"]
                exe_data["mode"] = self.mode
                exe_data["policy"] = self.policy
                # > add policy settings
                exe_data["policy_settings"] = {"max_runtime": self.config["run"]["job_max_runtime"]}
                for k, v in self.config["exe"]["policy_settings"].items():
                    if k == f"{str(exe_data['policy']).lower()}_template":
                        exe_data["policy_settings"][k] = str(self._local(v).absolute())
                    else:
                        exe_data["policy_settings"][k] = v
                exe_data["ncall"] = self.ncall
                exe_data["niter"] = self.niter
                # > create the runcard
                run_file: Path = self.job_path / DBRunner._file_run
                template = RuncardTemplate(self._local(self.config["run"]["template"]))
                channel_region: str = ""
                if db_jobs[0].part.region:
                    channel_region: str = f"region = {db_jobs[0].part.region}"
                template.fill(
                    run_file,
                    sweep=f"{self.mode!s} = {self.ncall}[{self.niter}]",
                    run="",
                    channels=db_jobs[0].part.string,
                    channels_region=channel_region,
                    toplevel="",
                )
                exe_data["input_files"] = [DBRunner._file_run]
                # > get last warmup (LW)
                LW = session.scalars(
                    select(Job)
                    .where(Job.part_id == self.part_id)
                    .where(Job.mode == ExecutionMode.WARMUP)
                    .where(Job.status == JobStatus.DONE)
                    .order_by(Job.id.desc())
                ).first()
                if not LW and self.mode == ExecutionMode.PRODUCTION:
                    raise RuntimeError(f"no warmup found for production job {self.part_name}")
                # > copy grid files
                if LW:
                    if not LW.rel_path:
                        raise RuntimeError(f"last warmup {LW.id} has no path")
                    LW_path: Path = self._local(LW.rel_path)
                    LW_data: ExeData = ExeData(LW_path)
                    if not LW_data.is_final:
                        raise RuntimeError(f"last warmup {LW.id} is not final")
                    for wfile in LW_data["output_files"]:
                        # > skip "*.s<seed>.*" files & job files
                        if re.match(r"^.*\.s[0-9]+\.[^0-9.]+$", wfile):
                            continue
                        if re.match(r"^job.*$", wfile):
                            continue
                        if self.mode == ExecutionMode.PRODUCTION and re.match(r"^.*\.txt$", wfile):
                            continue
                        shutil.copyfile(LW_path / wfile, self.job_path / wfile)
                        exe_data["input_files"].append(wfile)
                exe_data["output_files"] = []
                # > populate jobs datastructure
                exe_data["jobs"] = {}
                for db_job in db_jobs:
                    exe_data["jobs"][db_job.id] = {"seed": db_job.seed}
                # >  save to tmp file (this also updates the timestamp!)
                exe_data.write()
                # > commit update
                for db_job in db_jobs:
                    db_job.rel_path = str(self.job_path.relative_to(self._path))
                    db_job.status = JobStatus.RUNNING
                self._safe_commit(session)
            # > END IF DISPATCHED

            # get this from exe_data, only one for the batch? param: in_path?
            # just write a separate DBRecover task just taking a path and be done with it
            # or yield here the DB recover task
            self._debug(
                session,
                self._logger_prefix + f"::run:  yield Executor {exe_data['jobs']}",
            )
            yield Executor.factory(
                policy=self.policy,
                path=str(self.job_path.absolute()),
                log_level=self.config["ui"]["log_level"],
            )

            # > parse the retun data
            if not exe_data.is_final:
                # > even failed jobs should finalize ExeData
                raise RuntimeError(f"{self.ids} not final?!\n{self.job_path}\n{exe_data.data}")
            # > check if there was an Executor log written out; if yes print it
            exe_log: Path = exe_data.path / Executor._file_log
            if exe_log.exists():
                with open(exe_log, "r") as f:
                    self._logger(
                        session,
                        self._logger_prefix
                        + f"::run: Executor log [dim]({exe_log})[/dim]:\n"
                        + "\n".join(f" | [dim]{ln.strip()}[/dim]" for ln in f.readlines()),
                    )
            for db_job in db_jobs:  # loop exe data jobs keys
                if db_job.status in JobStatus.terminated_list():
                    continue
                if db_job.id in exe_data["jobs"] and "result" in exe_data["jobs"][db_job.id]:
                    if math.isnan(
                        float(exe_data["jobs"][db_job.id]["result"])
                        * float(exe_data["jobs"][db_job.id]["error"])
                    ):
                        db_job.status = JobStatus.FAILED
                    else:
                        db_job.result = exe_data["jobs"][db_job.id]["result"]
                        db_job.error = exe_data["jobs"][db_job.id]["error"]
                        db_job.chi2dof = exe_data["jobs"][db_job.id]["chi2dof"]
                        if exe_data["jobs"][db_job.id]["elapsed_time"] > 0:
                            db_job.elapsed_time = exe_data["jobs"][db_job.id]["elapsed_time"]
                        else:
                            # > log warning and keep estimated runtime in database to avoid issues down the line
                            self._logger(
                                session,
                                self._logger_prefix
                                + f"::run:  job {db_job.id} at {exe_data.path} has"
                                + f" elapsed time: {exe_data['jobs'][db_job.id]['elapsed_time']}"
                                + f" -> overwrite {db_job.elapsed_time}",
                                LogLevel.WARN,
                            )
                        db_job.status = JobStatus.DONE
                else:
                    db_job.status = JobStatus.FAILED
            self._safe_commit(session)

            # > see if a re-merge is possible
            if self.mode == ExecutionMode.PRODUCTION:
                mrg_part = self.clone(MergePart, force=False, part_id=self.part_id)
                if mrg_part.complete():
                    self._debug(session, self._logger_prefix + "::run:  MergePart complete")
                    return
                else:
                    self._logger(session, self._logger_prefix + "::run:  yield MergePart")
                    yield mrg_part
