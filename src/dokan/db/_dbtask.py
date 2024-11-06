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
from sqlalchemy.orm import Session #, scoped_session, sessionmaker

from rich.console import Console

from ._jobstatus import JobStatus
from ._loglevel import LogLevel
from ._sqla import DokanDB, Part, Job, Log

from ..task import Task
from ..exe import ExecutionMode, ExecutionPolicy, ExeData, Executor
from ..order import Order
from ..runcard import Runcard, RuncardTemplate


_console = Console()


class DBTask(Task, metaclass=ABCMeta):
    """the task class to interact with the database"""

    run_tag: float = luigi.FloatParameter()

    # > threadsafety using resource = 1, where read/write needed
    resources = {"DBTask": 1}
    # > database queries should jump the scheduler queue?
    # priority = 100

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # @todo all DBTasks need to be started in the job root path: check?
        self.dbname: str = "sqlite:///" + str(self._local("db.sqlite"))

    @property
    def engine(self) -> Engine:
        return create_engine(
            self.dbname +
            "?nolock=0",
            #connect_args={"check_same_thread": False, "timeout": 30.0},
            connect_args={"check_same_thread": True, "uri": True},
        )

    @property
    def session(self) -> Session:
        return Session(self.engine)
        # > scoped session needed for threadsafety
        # Session = scoped_session(sessionmaker(self.engine))
        # return Session()

    def output(self):
        # > DBTask has no output files but uses the DB itself to track the status
        return []

    @abstractmethod
    def complete(self) -> bool:
        return False

    def print_part(self) -> None:
        with self.session as session:
            for pt in session.scalars(select(Part)):
                print(pt)

    def print_job(self) -> None:
        with self.session as session:
            for job in session.scalars(select(Job)):
                print(job)

    def logger(self, message: str, level: LogLevel = LogLevel.INFO) -> None:
        if level >= 0 and level < self.config["ui"]["log_level"]:
            # > negative values are signals that I want passed *always*
            return
        if not self.config["ui"]["monitor"]:
            dt_str: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _console.print(f"[dim][{dt_str}][/dim]({level!r}): {message}")
            return
        # > general case: monitor is ON: store messages in DB
        with self.session as session:
            session.add(Log(level=level, timestamp=time.time(), message=message))
            session.commit()

    def debug(self, message: str) -> None:
        self.logger(message, LogLevel.DEBUG)

    def remainders(self) -> tuple[int, float]:
        with self.session as session:
            # > remaining resources available
            query_alloc = (  # active contains time estimates
                session.query(Job)
                .join(Part)
                .filter(Part.active.is_(True))
                .filter(Job.run_tag == self.run_tag)
                .filter(Job.mode == ExecutionMode.PRODUCTION)
                .filter(Job.status.in_(JobStatus.success_list() + JobStatus.active_list()))
            )
            njobs_alloc: int = query_alloc.count()
            njobs_rem: int = self.config["run"]["jobs_max_total"] - njobs_alloc
            T_alloc: float = sum(job.elapsed_time for job in query_alloc)
            T_rem: float = (
                self.config["run"]["jobs_max_total"] * self.config["run"]["job_max_runtime"]
                - T_alloc
            )
            return njobs_rem, T_rem

    # @todo make return a UserDict class with a schema?
    def distribute_time(self, T: float) -> dict:
        with self.session as session:
            # > cache information for the E-L formula and populate
            # > accumulators for an estimate for time per event
            cache = {}
            select_job = (
                select(Job)
                .join(Part)
                .where(Part.active.is_(True))
                .where(Job.status.in_(JobStatus.success_list() + JobStatus.active_list()))
                .where(Job.mode == ExecutionMode.PRODUCTION)
                .where(Job.policy == self.config["exe"]["policy"])
            )
            # > PreProduction assures there's a pre-production job for any new policy
            for job in session.scalars(select_job):
                if job.part_id not in cache:
                    cache[job.part_id] = {
                        "Ttot": job.part.Ttot,
                        "ntot": job.part.ntot,
                        "result": job.part.result,
                        "error": job.part.error,
                        "Textra": 0.0,
                        "nextra": 0,
                        "sum": 0.0,
                        "sum2": 0.0,
                        "norm": 0,
                        "count": 0,
                    }
                ntot: int = job.niter * job.ncall
                if job.status in JobStatus.success_list():
                    cache[job.part_id]["sum"] += job.elapsed_time
                    cache[job.part_id]["sum2"] += (job.elapsed_time) ** 2 / float(ntot)
                    cache[job.part_id]["norm"] += ntot
                    cache[job.part_id]["count"] += 1
                if job.status != JobStatus.MERGED:
                    # > everything that was not yet merged needs to be accounted for
                    # > in the error estimation & the distribution of *new* jobs
                    cache[job.part_id]["Textra"] += job.elapsed_time
                    cache[job.part_id]["nextra"] += ntot

            # > check every active part has an entry
            for pt in session.scalars(select(Part).where(Part.active.is_(True))):
                if pt.id not in cache:
                    raise RuntimeError(f"part {pt.id} not in cache?!")

            # > actually compute estimate for time per event
            # > populate accumulators to evaluate the E-L optimization formula
            result = {
                "part": {},  # part_id -> {tau, tau_err, T_opt, T_max_job, T_job, njobs, ntot_job}
                "tot_result": 0.0,
                "tot_error": 0.0,
                "tot_error_estimate_opt": 0.0,
                "tot_error_estimate_jobs": 0.0,
            }
            accum_T: float = T
            accum_err_sqrtT: float = 0.0
            for part_id, ic in cache.items():
                if part_id not in result["part"]:
                    i_tau: float = ic["sum"] / ic["norm"]
                    i_tau_err: float = 0.0
                    if ic["count"] > 1:
                        i_tau_err = ic["sum2"] / ic["norm"] - i_tau**2
                        if i_tau_err <= 0.0:
                            i_tau_err = 0.0
                        else:
                            i_tau_err = math.sqrt(i_tau_err)
                    # > convert to time
                    # include estimate from the extra jobs already allocated
                    i_T: float = i_tau * (ic["ntot"] + ic["nextra"])
                    ic["error"] = math.sqrt(
                        ic["error"] ** 2 * ic["ntot"] / (ic["ntot"] + ic["nextra"])
                    )
                    result["part"][part_id] = {
                        "tau": i_tau,
                        "tau_err": i_tau_err,
                        "i_T": i_T,
                        "i_err_sqrtT": ic["error"] * math.sqrt(i_T),
                    }
                else:
                    raise RuntimeError(f"part {part_id} already in result?!")
                accum_T += result["part"][part_id]["i_T"]
                accum_err_sqrtT += result["part"][part_id]["i_err_sqrtT"]

            # > use E-L formula to compute the optimal distribution of T to the active parts
            acc_T_opt: float = 0.0
            for _, ires in result["part"].items():
                i_err_sqrtT: float = ires.pop("i_err_sqrtT")
                i_T: float = ires.get("i_T")  # need it for error calc below
                T_opt: float = (i_err_sqrtT / accum_err_sqrtT) * accum_T - i_T
                if T_opt < 0.0:
                    T_opt = 0.0  # too lazy to do proper inequality E-L optimization
                ires["T_opt"] = T_opt
                acc_T_opt += T_opt
            # > normalize at the end to account for the dropped negative weights
            # and compute an estimate for the error to be achieved
            result["tot_result"] = 0.0
            result["tot_error"] = 0.0
            result["tot_error_estimate_opt"] = 0.0
            for part_id, ires in result["part"].items():
                ires["T_opt"] *= T / acc_T_opt
                i_T: float = ires.get("i_T")
                result["tot_result"] += cache[part_id]["result"]
                result["tot_error"] += cache[part_id]["error"] ** 2
                result["tot_error_estimate_opt"] += (
                    cache[part_id]["error"] ** 2 * i_T / (i_T + ires["T_opt"])
                )
            result["tot_error"] = math.sqrt(result["tot_error"])
            result["tot_error_estimate_opt"] = math.sqrt(result["tot_error_estimate_opt"])

            # > split up into jobs
            # (T_max_job, T_job, njobs, ntot_job)
            result["tot_error_estimate_jobs"] = 0.0
            for part_id, ires in result["part"].items():
                # > 3.5 sigma buffer but never larger than 50% runtime
                tau_buf: float = min(3.5 * ires["tau_err"], 0.5 * ires["tau"])
                if tau_buf == 0.0:  # in case we have no clue: target 50%
                    tau_buf = 0.5 * ires["tau"]
                # > target runtime for one job corrected for buffer
                T_max_job: float = self.config["run"]["job_max_runtime"] * (
                    1.0 - tau_buf / ires["tau"]
                )
                if self.config["run"]["job_fill_max_runtime"]:
                    njobs: int = round(ires["T_opt"] / T_max_job)
                    ntot_job: int = int(T_max_job / ires["tau"])
                else:
                    if ires["T_opt"] > 0.0:
                        njobs: int = int(ires["T_opt"] / T_max_job) + 1
                        ntot_job: int = int(ires["T_opt"] / float(njobs) / ires["tau"])
                    else:
                        njobs: int = 0
                        ntot_job: int = 0
                T_job: float = ntot_job * ires["tau"]
                T_jobs: float = njobs * T_job
                ires["T_max_job"] = T_max_job
                ires["T_job"] = T_job
                ires["njobs"] = njobs
                ires["ntot_job"] = ntot_job
                i_T: float = ires.pop("i_T")  # pop it here
                result["tot_error_estimate_jobs"] += (
                    cache[part_id]["error"] ** 2 * i_T / (i_T + T_jobs)
                )
            result["tot_error_estimate_jobs"] = math.sqrt(result["tot_error_estimate_jobs"])

            return result


class DBInit(DBTask):
    """initilization of the 'parts' table of the database with process channel information"""

    order: int = luigi.IntParameter(default=Order.NNLO)
    channels: dict = luigi.DictParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        DokanDB.metadata.create_all(self.engine)

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
        self.logger(f"DBInit: run {Order(self.order)!r}")
        with self.session as session:
            for db_pt in session.scalars(select(Part)):
                db_pt.active = False  # reset to be safe
            for pt in self.channels:
                stmt = select(Part).where(Part.name == pt)
                # @ todo catch case where it's already there and check it has same entries?
                if not session.scalars(stmt).first():
                    session.add(
                        Part(name=pt, active=False, timestamp=time.time(), **self.channels[pt])
                    )
                for db_pt in session.scalars(stmt):
                    db_pt.active = Order(db_pt.order).is_in(Order(self.order))
            session.commit()
        # self.print_part()


class DBDispatch(DBTask):
    # > inactive selection: 0
    # > pick a specific `Job` by id: > 0
    # > restrict to specific `Part` by id: < 0 [take abs]
    id: int = luigi.IntParameter(default=0)

    # > in order to be able to create multiple id==0 dispatchers,
    # need an additional parameter to distinguish them
    _n: int = luigi.IntParameter(default=0)

    # > mode and policy must be set already before dispatch!

    @property
    def resources(self):
        if self.id == 0:
            return {"DBDispatch": 1}
        else:
            return None

    # priority = 10

    @property
    def select_job(self):
        # > define the selector for the jobs based on the id that was passed & filter by the run_tag
        slct = select(Job).where(Job.run_tag == self.run_tag)
        if self.id > 0:
            return slct.where(Job.id == self.id)
        elif self.id < 0:
            return slct.where(Job.part_id == abs(self.id))
        else:
            return slct

    def complete(self) -> bool:
        with self.session as session:
            if (
                session.scalars(self.select_job.where(Job.status == JobStatus.QUEUED)).first()
                is not None
            ):
                return False
            return True

    def repopulate(self):
        if self.id != 0:
            return

        njobs_rem, T_rem = self.remainders()
        if njobs_rem <= 0 or T_rem <= 0.0:
            return

        with self.session as session:
            # > queue up a new production job in the database and return job id's
            def queue_production(part_id: int, opt: dict) -> list[int]:
                if opt["njobs"] <= 0:
                    return []
                niter: int = self.config["production"]["niter"]
                ncall: int = opt["ntot_job"] // niter
                if ncall * niter == 0:
                    self.info(f"part {part_id} has ntot={opt['ntot_job']} -> 0 = {ncall} * {niter}")
                    return []
                jobs: list[Job] = [
                    Job(
                        run_tag=self.run_tag,
                        part_id=part_id,
                        mode=ExecutionMode.PRODUCTION,
                        policy=self.config["exe"]["policy"],
                        status=JobStatus.QUEUED,
                        timestamp=0.0,
                        ncall=ncall,
                        niter=niter,
                        elapsed_time=opt["T_job"],  # a time estimate
                    )
                    for _ in range(opt["njobs"])
                ]
                session.add_all(jobs)
                session.commit()
                return [job.id for job in jobs]

            self.debug(f"DBDispatch[{self._n}]: repopulate {self.id} | {self.run_tag}")
            self.debug(f"njobs  - remaining: {njobs_rem}")
            self.debug(f"T      - remaining: {T_rem}")

            # > build up subquery to get Parts with job counts
            def job_count_subquery(js_list: list[JobStatus]):
                return (
                    session.query(Job.part_id, func.count(Job.id).label("job_count"))
                    .filter(Job.run_tag == self.run_tag)
                    .filter(Job.mode == ExecutionMode.PRODUCTION)
                    .filter(Job.status.in_(js_list))
                    .group_by(Job.part_id)
                    .subquery()
                )

            # > populate until some termination condition is reached
            while True:
                if njobs_rem <= 0 or T_rem <= 0.0:
                    return

                # > get counters for temrination conditions on #queued
                job_count_queued = job_count_subquery([JobStatus.QUEUED])
                job_count_active = job_count_subquery(JobStatus.active_list())
                job_count_success = job_count_subquery(JobStatus.success_list())
                # > get tuples (Part, #queued, #active, #success) ordered by #queued
                sorted_parts = (
                    session.query(
                        Part,  # Part.id only?
                        job_count_queued.c.job_count,
                        job_count_active.c.job_count,
                        job_count_success.c.job_count,
                    )
                    .outerjoin(job_count_queued, Part.id == job_count_queued.c.part_id)
                    .outerjoin(job_count_active, Part.id == job_count_active.c.part_id)
                    .outerjoin(job_count_success, Part.id == job_count_success.c.part_id)
                    .filter(Part.active.is_(True))
                    .order_by(job_count_queued.c.job_count.desc())
                    .all()
                )

                # > termination condition based on #queued of individul jobs
                qbreak: bool = False
                for pt, nque, nact, nsuc in sorted_parts:
                    self.debug(f"  >> {pt!r} | {nque} | {nact} | {nsuc}")
                    if not nque:
                        continue
                    # > implement termination conditions
                    if nque >= self.config["run"]["jobs_batch_size"]:
                        qbreak = True
                    nsuc = nsuc if nsuc else 0
                    nact = nact if nact else 0
                    # can i do:  nsuc = nsuc or 0
                    if nque >= 2 * (nsuc + (nact - nque)):
                        qbreak = True
                    # @todo: more?
                if qbreak:
                    break

                # for pt in session.scalars(select(Part).where(Part.active.is_(True))):
                #     print(pt)
                #     print(sum(1 for j in pt.jobs if j.status == JobStatus.DONE))

                # > register new jobs
                T_next: float = min(
                    self.config["run"]["jobs_batch_size"] * self.config["run"]["job_max_runtime"],
                    njobs_rem * self.config["run"]["job_max_runtime"],
                    T_rem,
                )
                opt_dist: dict = self.distribute_time(T_next)
                rel_acc: float = abs(opt_dist["tot_error"] / opt_dist["tot_result"])
                if rel_acc <= self.config["run"]["target_rel_acc"]:
                    break
                while (
                    tot_njobs := sum(opt["njobs"] for opt in opt_dist["part"].values())
                ) > njobs_rem:
                    min_njobs: int = min(
                        opt["njobs"] for opt in opt_dist["part"].values() if opt["njobs"] > 0
                    )
                    for opt in opt_dist["part"].values():
                        if opt["njobs"] > 0:
                            opt["njobs"] -= min_njobs
                tot_T: float = 0.0
                for part_id, opt in sorted(
                    opt_dist["part"].items(), key=lambda x: x[1]["T_opt"], reverse=True
                ):
                    if tot_njobs == 0:
                        # > at least one job: pick largest T_opt one
                        opt["njobs"] = 1
                        tot_njobs = 1  # trigger only 1st iteration
                    self.debug(f"{part_id}: {opt}")
                    if opt["njobs"] <= 0:
                        continue
                    # > regiser njobs new jobs with ncall,niter and time estime to DB
                    ids = queue_production(part_id, opt)
                    self.logger(f"DBDispatch[{self._n}]: queued[{part_id}]: {len(ids)} = {ids}")
                    tot_njobs += opt["njobs"]
                    tot_T += opt["njobs"] * opt["T_job"]

                # > commit & update remaining resources for next iteration
                session.commit()
                njobs_rem -= tot_njobs
                T_rem -= tot_T

                estimate_rel_acc: float = abs(
                    opt_dist["tot_error_estimate_jobs"] / opt_dist["tot_result"]
                )
                if estimate_rel_acc <= self.config["run"]["target_rel_acc"]:
                    break

    def run(self):
        # print(f"DBDispatch: run {self.id}")

        self.repopulate()

        with self.session as session:
            # > get the front of the queue
            stmt = self.select_job.where(Job.status == JobStatus.QUEUED).order_by(Job.id.asc())
            # @todo add batches
            job = session.scalars(stmt).first()
            if job:
                # @todo set seeds here! (batch size rounding and ordering)
                # > get last job that has a seed assigned to it
                last_job = session.scalars(
                    select(Job)
                    .where(Job.part_id == job.part_id)
                    .where(Job.mode == job.mode)
                    .where(Job.seed.is_not(None))
                    .where(Job.seed > self.config["run"]["seed_offset"])
                    # @todo not good enough, need a max to shield from anothe batch-jobstarting at larger value of seed?
                    # determine upper bound by the max number of jobs? -> seems like a good idea
                    .order_by(Job.seed.desc())
                ).first()
                if last_job:
                    # print(f"{self.id} last job:\n>  {last_job!r}")
                    seed_start: int = last_job.seed + 1
                else:
                    seed_start: int = self.config["run"]["seed_offset"] + 1
                job.seed = seed_start
                job.status = JobStatus.DISPATCHED
                session.commit()
                # if self.id == 0:
                #     self.decrease_running_resources({"DBDispatch": 1})
                self.logger(f"DBDispatch[{self._n}]: submitting {job!r}")
                yield self.clone(cls=DBRunner, id=job.id)


class DBRunner(DBTask):
    # @todo make a list to accommodate batch jobs
    id: int = luigi.IntParameter()
    # priority = 100

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # @todo find the part id & check they are the same for all in the batch
        # @todo check that the seeds are sequential

    def complete(self) -> bool:
        with self.session as session:
            job: Job = session.get_one(Job, self.id)
            # @todo add classmethod JobStatus to query properties
            return job.status in JobStatus.terminated_list()

    def run(self):
        # self.logger(f"DBRunner: run {self.id}")
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
                # @todo: add policy settings
                exe_data["policy_settings"] = {}
                if db_job.policy == ExecutionPolicy.LOCAL:
                    exe_data["policy_settings"]["local_ncores"] = 1
                elif db_job.policy == ExecutionPolicy.HTCONDOR:
                    exe_data["policy_settings"]["htcondor_id"] = 42
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
                        # > skip "*.s<seed>.*" files
                        if re.match(r"^.*\.s[0-9]+\.[^0-9.]+$", wfile):
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

            # nope: cyclic dependence #> see if a re-merge is possible
            # yield MergePart(part_id=db_job.part_id)
