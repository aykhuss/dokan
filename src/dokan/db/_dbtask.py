import datetime
import math
import time
from abc import ABCMeta, abstractmethod

import luigi
from rich.console import Console
from sqlalchemy import Engine, create_engine, select
from sqlalchemy.orm import Session  # , scoped_session, sessionmaker

from ..exe import ExecutionMode
from ..order import Order
from ..task import Task
from ._jobstatus import JobStatus
from ._loglevel import LogLevel
from ._sqla import DokanDB, DokanLog, Job, Log, Part

_console = Console()


class DBTask(Task, metaclass=ABCMeta):
    """the task class to interact with the database"""

    run_tag: float = luigi.FloatParameter()

    # > threadsafety using resource = 1, where read/write needed
    resources = {"DBTask": 1}
    # > database queries should jump the scheduler queue?

    # priority = 1

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # @todo all DBTasks need to be started in the job root path: check?
        self.dbname: str = "sqlite:///" + str(self._local("db.sqlite").absolute())
        self.logname: str = "sqlite:///" + str(self._local("log.sqlite").absolute())

    def _create_engine(self, name: str) -> Engine:
        return create_engine(name + "?timeout=1000&uri=true")

    @property
    def session(self) -> Session:
        return Session(
            binds={
                DokanDB: self._create_engine(self.dbname),
                DokanLog: self._create_engine(self.logname),
            }
        )

    def _safe_commit(self, session: Session) -> None:
        # for _ in range(10):  # maximum number of tries
        #     try:
        #         session.commit()
        #         return
        #     except Exception as e:
        #         self._logger(session, f"DBTask::_safe_commit: {e!r}", LogLevel.ERROR)
        #         time.sleep(1.0)  # time delay between retries
        # raise RuntimeError("DBTask::_safe_commit: ran out of retries")
        # > the above did not work well, rely on the SQLite timeout instead
        session.commit()

    def output(self):
        # > DBTask has no output files but uses the DB itself to track the status
        return []

    @abstractmethod
    def complete(self) -> bool:
        return False

    def _clear_log(self):
        with self.session as session:
            for log in session.scalars(select(Log)):
                session.delete(log)
            self._safe_commit(session)

    def _print_part(self, session: Session) -> None:
        for pt in session.scalars(select(Part)):
            print(pt)

    def _print_job(self, session: Session) -> None:
        for job in session.scalars(select(Job)):
            print(job)

    def _logger(self, session: Session, message: str, level: LogLevel = LogLevel.INFO) -> None:
        # > negative values are signals: always store in databese (workflow relies on this)
        if level < 0:
            session.add(Log(level=level, timestamp=time.time(), message=message))
            self._safe_commit(session)
        # > pass through log level & all signales
        if level >= 0 and level < self.config["ui"]["log_level"]:
            return
        # > print out
        if not self.config["ui"]["monitor"]:
            dt_str: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _console.print(f"(c)[dim][{dt_str}][/dim]({level!r}): {message}")
            return
        # > general case: monitor is ON: store messages in DB
        last_log = session.scalars(select(Log).order_by(Log.id.desc())).first()
        if last_log and last_log.level in [LogLevel.SIG_COMP]:
            dt_str: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _console.print(f"(c)[dim][{dt_str}][/dim]({level!r}): {message}")
        elif level >= 0:
            session.add(Log(level=level, timestamp=time.time(), message=message))
            self._safe_commit(session)

    def _debug(self, session: Session, message: str) -> None:
        self._logger(session, message, LogLevel.DEBUG)

    def _remainders(self, session: Session) -> tuple[int, float]:
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
            self.config["run"]["jobs_max_total"] * self.config["run"]["job_max_runtime"] - T_alloc
        )
        return njobs_rem, T_rem

    # @todo make return a UserDict class with a schema?
    def _distribute_time(self, session: Session, T: float) -> dict:
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
                if job.elapsed_time < 0.0:
                    self._logger(
                        session,
                        "DBTask::_distribute_time: skipping negative elapsed time in " + f"{job!r}",
                        LogLevel.WARN,
                    )
                    continue
                cache[job.part_id]["sum"] += job.elapsed_time
                cache[job.part_id]["sum2"] += (job.elapsed_time) ** 2 / float(ntot)
                cache[job.part_id]["norm"] += ntot
                cache[job.part_id]["count"] += 1
            if job.status != JobStatus.MERGED:
                # > everything that was not yet merged needs to be accounted for
                # > in the error estimation & the distribution of *new* jobs
                cache[job.part_id]["Textra"] += job.elapsed_time
                cache[job.part_id]["nextra"] += ntot

        # > check every active part has an entry; compute the minimum & average error
        pt_min_error: float = +float("inf")
        pt_max_error: float = -float("inf")
        pt_avg_error: float = 0.0  # avg error on part to get target accuracy
        for pt in session.scalars(select(Part).where(Part.active.is_(True))):
            if pt.id not in cache:
                raise RuntimeError(f"part {pt.id} not in cache?!")
            if cache[pt.id]["error"] > 0.0:
                pt_min_error = min(pt_min_error, cache[pt.id]["error"])
                pt_max_error = max(pt_max_error, cache[pt.id]["error"])
            pt_avg_error += cache[pt.id]["result"]
        pt_avg_error = (
            self.config["run"]["target_rel_acc"] * pt_avg_error / math.sqrt(len(cache) + 1.0)
        )
        # > override errors so they are always non-zero; penalize pre-production only parts
        # _console.print(cache)
        for part_id, ic in cache.items():
            # if ic["error"] < min(pt_avg_error, pt_min_error):
            #     ic["error"] += 2e-3 * min(pt_avg_error, pt_min_error)
            if ic["error"] < 2 * pt_min_error:
                ic["error"] += 0.5 * pt_min_error
            if ic["count"] <= 1 and ic["nextra"] <= 0:
                ic["error"] += pt_max_error
        # _console.print(cache)

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
                        # i_tau_err = 0.0
                        i_tau_err = abs(i_tau_err)  # keep as an estimate
                    else:
                        i_tau_err = math.sqrt(i_tau_err)
                # > convert to time
                # include estimate from the extra jobs already allocated
                i_T: float = i_tau * (ic["ntot"] + ic["nextra"])
                ic["error"] = math.sqrt(ic["error"] ** 2 * ic["ntot"] / (ic["ntot"] + ic["nextra"]))
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

        # > use E-L formula to compute a time estimate (beyond T)
        # > needed to achieve the desired accuracy
        target_abs_acc: float = self.config["run"]["target_rel_acc"] * result["tot_result"]
        result["T_target"] = max(0.0, (accum_err_sqrtT / target_abs_acc) ** 2 - accum_T)

        # > split up into jobs
        # (T_max_job, T_job, njobs, ntot_job)
        result["tot_error_estimate_jobs"] = 0.0
        for part_id, ires in result["part"].items():
            # > 5 sigma buffer but never larger than 50% runtime
            tau_buf: float = min(5 * ires["tau_err"], 0.5 * ires["tau"])
            if tau_buf <= 0.0:  # in case we have no clue (tau_err==0): target 50%
                tau_buf = 0.5 * ires["tau"]

            # > target runtime for one job corrected for buffer
            T_max_job: float = self.config["run"]["job_max_runtime"] * (1.0 - tau_buf / ires["tau"])
            if self.config["run"]["job_fill_max_runtime"]:
                njobs: int = round(ires["T_opt"] / T_max_job)
                ntot_job: int = int(T_max_job / ires["tau"])
            else:
                if ires["T_opt"] > 0.0:
                    ntot_min: int = (
                        self.config["production"]["niter"]
                        * self.config["production"]["ncall_start"]
                    )
                    ntot_max: int = int(T_max_job / ires["tau"])
                    njobs: int = int(ires["T_opt"] / T_max_job) + 1
                    ntot_job: int = int(ires["T_opt"] / float(njobs) / ires["tau"])
                    ntot_job = min(ntot_max, max(ntot_min, ntot_job))
                else:
                    njobs: int = 0
                    ntot_job: int = 0

            # > if we inflated the error of a count==1 part, we only want to register *one* job
            if cache[part_id]["count"] <= 1 and cache[part_id]["nextra"] <= 0:
                njobs = min(njobs, 1)

            # > update & store info for each part
            T_job: float = ntot_job * ires["tau"]
            T_jobs: float = njobs * T_job
            ires["T_max_job"] = T_max_job
            ires["T_job"] = T_job
            ires["njobs"] = njobs
            ires["ntot_job"] = ntot_job
            i_T: float = ires.pop("i_T")  # pop it here
            result["tot_error_estimate_jobs"] += cache[part_id]["error"] ** 2 * i_T / (i_T + T_jobs)

        result["tot_error_estimate_jobs"] = math.sqrt(result["tot_error_estimate_jobs"])

        return result


class DBInit(DBTask):
    """initialization of the databases

    create databases if they do not exist yet. populate the `parts` table
    with the channels information and set the `active` state according to
    the requested order.

    Attributes
    ----------
    order : int
        the order of the calculation according to the `Order` IntEnum
    channels : dict
        channel description as parsed from the `NNLOJER -listlumi <PROC>` output
    """

    order: int = luigi.IntParameter(default=Order.NNLO)
    channels: dict = luigi.DictParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        DokanDB.metadata.create_all(self._create_engine(self.dbname))
        DokanLog.metadata.create_all(self._create_engine(self.logname))

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
        with self.session as session:
            # self._logger(session, f"DBInit::run order = {Order(self.order)!r}")
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
            self._safe_commit(session)
        # self.print_part()
