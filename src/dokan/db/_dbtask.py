import datetime
import math
import time
from abc import ABCMeta, abstractmethod

import luigi
from rich.console import Console
from sqlalchemy import Engine, create_engine, select, text
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
        self.db_setup: bool = False

    def _create_engine(self, name: str) -> Engine:
        """Create a SQLite engine with WAL mode and concurrency settings."""
        engine = create_engine(name, connect_args={"timeout": 1800})

        # > Apply concurrency-friendly SQLite PRAGMAs
        if self.db_setup:
            with engine.connect() as conn:
                conn.execute(text("PRAGMA journal_mode=WAL;"))
                conn.execute(text("PRAGMA synchronous=NORMAL;"))
                # conn.execute(text("PRAGMA wal_autocheckpoint=1000;"))
                # conn.execute(text("PRAGMA busy_timeout=30000;"))
                conn.execute(text("PRAGMA temp_store=MEMORY;"))

        return engine

    @property
    def session(self) -> Session:
        return Session(
            binds={
                DokanDB: self._create_engine(self.dbname),
                DokanLog: self._create_engine(self.logname),
            },
            autoflush=False,
        )

    def _safe_commit(self, session: Session) -> None:
        from sqlalchemy.exc import OperationalError

        dt_str: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for i in range(10):  # maximum number of tries
            try:
                session.commit()
                return
            except OperationalError as e:
                if "database is locked" in str(e):
                    _console.print(
                        f"(c)[dim][{dt_str}][/dim](WARN): DBTask::_safe_commit locked, retrying..."
                    )
                    time.sleep(1.0 + i * 0.5)  # exponential backoff
                    continue
                raise e
            except Exception as e:
                # > do NOT use self._logger here to avoid recursion loop
                _console.print(f"(c)[dim][{dt_str}][/dim](ERROR): DBTask::_safe_commit: {e!r}")
                time.sleep(1.0)  # time delay between retries
        raise RuntimeError("DBTask::_safe_commit: ran out of retries")

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
        dt_str: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if not self.config["ui"]["monitor"]:
            _console.print(f"(c)[dim][{dt_str}][/dim]({level!r}): {message}")
            return
        # > general case: monitor is ON: store messages in DB
        last_log = session.scalars(select(Log).order_by(Log.id.desc())).first()
        if last_log and last_log.level in [LogLevel.SIG_COMP]:
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
        T_rem: float = self.config["run"]["jobs_max_total"] * self.config["run"]["job_max_runtime"] - T_alloc
        return njobs_rem, T_rem

    # @todo make return a UserDict class with a schema?
    def _distribute_time(self, session: Session, T: float) -> dict:
        from sqlalchemy.orm import joinedload

        # > cache information for the E-L formula and populate
        # > accumulators for an estimate for time per event
        cache = {}
        select_job = (
            select(Job)
            .options(joinedload(Job.part))
            .join(Part)
            .where(Part.active.is_(True))
            .where(Job.status.in_(JobStatus.success_list() + JobStatus.active_list()))
            .where(Job.mode == ExecutionMode.PRODUCTION)
            .where(Job.policy == self.config["exe"]["policy"])
        )
        # > PreProduction guarantees there's a production job for any new policy
        for job in session.scalars(select_job):
            if job.part_id not in cache:
                cache[job.part_id] = {
                    "Ttot": job.part.Ttot,
                    "ntot": job.part.ntot,
                    "result": job.part.result,
                    "error": job.part.error,
                    "adj_error": float("nan"),
                    "Textra": 0.0,
                    "nextra": 0,
                    "sum": 0.0,
                    "sum2": 0.0,
                    "norm": 0,
                    "count": 0,
                }
            if job.elapsed_time < 0.0:
                self._logger(
                    session,
                    "DBTask::_distribute_time:  skipping negative elapsed time in " + f"{job!r}",
                    LogLevel.WARN,
                )
                continue
            ntot: int = job.niter * job.ncall
            # > runtime estimate based on *all* successful jobs
            if job.status in JobStatus.success_list():
                # >--------
                # A > previously we weighted the longer jobs with a heigher weight
                # A > but this could lead to a bias towards the runtime-limit
                # cache[job.part_id]["sum"] += job.elapsed_time
                # cache[job.part_id]["sum2"] += (job.elapsed_time) ** 2 / float(ntot)
                # cache[job.part_id]["norm"] += ntot
                # B > now we just do a standard sample average
                itau: float = job.elapsed_time / float(ntot)
                cache[job.part_id]["sum"] += itau
                cache[job.part_id]["sum2"] += itau**2
                cache[job.part_id]["norm"] += 1
                # >--------
                cache[job.part_id]["count"] += 1
            # > extra time allocation from active parts & DONE jobs
            if job.status in [*JobStatus.active_list(), JobStatus.DONE]:
                # > everything that was not yet merged needs to be accounted for
                # > in the error estimation & the distribution of *new* jobs
                cache[job.part_id]["Textra"] += job.elapsed_time
                cache[job.part_id]["nextra"] += ntot
            # @todo maybe we would want to include the failed jobs above
            #  to see if they hit the runtime limit?

        # > check every active part has an entry; compute the minimum & average error; accumulate tot result & error
        pt_min_error: float = +float("inf")
        pt_max_error: float = -float("inf")
        pt_avg_error: float = 0.0  # avg error on part to get target accuracy
        tot_result: float = 0.0
        tot_error: float = 0.0
        for pt in session.scalars(select(Part).where(Part.active.is_(True))):
            if pt.id not in cache:
                raise RuntimeError(f"part {pt.id} not in cache?!")
            if cache[pt.id]["error"] > 0.0:
                pt_min_error = min(pt_min_error, cache[pt.id]["error"])
                pt_max_error = max(pt_max_error, cache[pt.id]["error"])
            pt_avg_error += abs(cache[pt.id]["result"])
            tot_result += cache[pt.id]["result"]
            tot_error += cache[pt.id]["error"] ** 2
        # > at this point, `pt_avg_error` = sum_{pt}(|result_pt|)
        pt_max_error = max(pt_max_error, self.config["run"]["target_rel_acc"] * pt_avg_error)
        pt_avg_error = self.config["run"]["target_rel_acc"] * pt_avg_error / math.sqrt(len(cache) + 1.0)
        tot_error = math.sqrt(tot_error)

        # > adjusted errors
        # _console.print(cache)
        adj_thresh_min: float = 2.0
        adj_thresh_max: float = 1e2
        adj_penalty: float = 10.0
        for part_id, ic in cache.items():
            ic["adj_error"] = ic["error"]
            # > enforce non-zero errors:  arithmetic mean
            if ic["error"] < adj_thresh_min * pt_min_error:
                ic["adj_error"] = 0.5 * (ic["error"] + adj_thresh_min * pt_min_error)
            # > dampen outliers:  geometric mean
            if ic["error"] > adj_thresh_max * pt_avg_error:
                ic["adj_error"] = math.sqrt(ic["error"] * adj_thresh_max * pt_avg_error)
            # > penalize pre-production only parts
            if ic["count"] <= self.config["production"]["min_number"] and ic["nextra"] <= 0:
                ic["adj_error"] = ic["error"] + adj_penalty * pt_max_error
                self._debug(
                    session,
                    f"DBTask::_distribute_time:  penalize error for part={part_id}: {ic['adj_error']}",
                )
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
        # > loop until there are no negative time assignments
        accum_T: float = 0.0
        accum_err_sqrtT: float = 0.0
        while True:
            accum_T = 0.0
            accum_err_sqrtT = 0.0
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
                    ic["adj_error"] = math.sqrt(
                        ic["adj_error"] ** 2 * ic["ntot"] / (ic["ntot"] + ic["nextra"])
                    )
                    result["part"][part_id] = {
                        "tau": i_tau,
                        "tau_err": i_tau_err,
                        "i_T": i_T,
                        "i_err_sqrtT": ic["adj_error"] * math.sqrt(i_T),
                    }
                # > skip excluded parts
                if result["part"][part_id].get("T_opt", 1.0) > 0.0:
                    accum_T += result["part"][part_id]["i_T"]
                    accum_err_sqrtT += result["part"][part_id]["i_err_sqrtT"]

            # > use E-L formula to compute the optimal distribution of T to the active parts
            # > and flag if parts were removed and we need to recompute
            acc_T_opt: float = 0.0
            no_negative_T_opt: bool = True
            for part_id, ires in result["part"].items():
                if ires.get("T_opt", 1.0) <= 0.0:
                    continue
                # i_err_sqrtT: float = ires.pop("i_err_sqrtT")
                i_err_sqrtT: float = ires.get("i_err_sqrtT")
                i_T: float = ires.get("i_T")  # need it for error calc below
                T_opt: float = (i_err_sqrtT / accum_err_sqrtT) * (T + accum_T) - i_T
                if T_opt < 0.0:
                    no_negative_T_opt = False
                    T_opt = 0.0  # flag as excluded from optimization
                ires["T_opt"] = T_opt
                acc_T_opt += T_opt
            # > check if all T_opt were positive
            if no_negative_T_opt:
                self._debug(
                    session,
                    f"DBTask::_distribute_time:  skipped: {[part_id for part_id, ires in result['part'].items() if ires['T_opt'] <= 0.0]}",
                )
                for _, ires in result["part"].items():
                    del ires["i_err_sqrtT"]
                break  # no more negative T_opt

        # > re-normalize at the end for good measure
        # > and compute an estimate for the error to be achieved
        self._debug(session, f"DBTask::_distribute_time:  {T=} v.s. {acc_T_opt=}")
        result["tot_result"] = 0.0
        result["tot_error"] = 0.0
        result["tot_adj_error"] = 0.0
        result["tot_error_estimate_opt"] = 0.0
        for part_id, ires in result["part"].items():
            if acc_T_opt > 0:
                ires["T_opt"] *= T / acc_T_opt
            i_T: float = ires.get("i_T")
            result["tot_result"] += cache[part_id]["result"]
            result["tot_error"] += cache[part_id]["error"] ** 2
            if math.isnan(cache[part_id]["adj_error"]):
                result["tot_adj_error"] += cache[part_id]["error"] ** 2
            else:
                result["tot_adj_error"] += cache[part_id]["adj_error"] ** 2
            result["tot_error_estimate_opt"] += cache[part_id]["error"] ** 2 * i_T / (i_T + ires["T_opt"])
        result["tot_error"] = math.sqrt(result["tot_error"])
        result["tot_adj_error"] = math.sqrt(result["tot_adj_error"])
        result["tot_error_estimate_opt"] = math.sqrt(result["tot_error_estimate_opt"])

        # > use E-L formula to compute a time estimate (beyond T)
        # > needed to achieve the desired accuracy
        target_abs_acc: float = abs(self.config["run"]["target_rel_acc"] * result["tot_result"])
        result["T_target"] = (accum_err_sqrtT / target_abs_acc) ** 2 - accum_T
        self._debug(
            session,
            f"DBTask::_distribute_time: tot_result = {result['tot_result']},  {target_abs_acc=}, T_target={result['T_target']}",
        )
        result["T_target"] = max(0.0, result["T_target"])

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
                        self.config["production"]["niter"] * self.config["production"]["ncall_start"]
                    )
                    ntot_max: int = int(T_max_job / ires["tau"])
                    njobs: int = int(ires["T_opt"] / T_max_job) + 1
                    ntot_job: int = int(ires["T_opt"] / float(njobs) / ires["tau"])
                    ntot_job = min(ntot_max, max(ntot_min, ntot_job))
                else:
                    njobs: int = 0
                    ntot_job: int = 0

            # > if we inflated the error of a count==1 part, we only want to register *one* job
            if (
                cache[part_id]["count"] <= self.config["production"]["min_number"]
                and cache[part_id]["nextra"] <= 0
            ):
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
