import datetime
import math
import os
import time
from abc import ABCMeta, abstractmethod
from enum import Enum
from functools import partial

import luigi
from rich.console import Console
from sqlalchemy import Engine, create_engine, event, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session  # , scoped_session, sessionmaker

from ..exe import ExecutionMode, ExecutionPolicy, ExeData
from ..task import Task
from ._jobstatus import JobStatus
from ._loglevel import LogLevel
from ._sqla import DokanDB, DokanLog, Job, Log, Part

_console = Console()

class _DBRole(Enum):
    """Which database an engine serves; determines its durability policy."""

    JOB = "job"  # db.sqlite  — workflow state, source of truth for scheduling
    LOG = "log"  # log.sqlite — event stream + workflow signals


# > empirically required on network storage: lock contention under many
# > concurrent workers is absorbed by the driver-level busy timeout, not by
# > session-level retries (see `DBTask._safe_commit`)
_SQLITE_TIMEOUT: int = 420  # seconds

# > all connection-scoped settings in one place, applied to *every* new
# > connection via the pool `connect` event — `journal_mode`, `synchronous`,
# > `temp_store`, and `foreign_keys` are per-connection PRAGMAs, so applying
# > them anywhere else silently misses pooled/forked connections.
# >
# > - journal_mode=PERSIST: same crash-safety as DELETE (no WAL shared memory,
# >   so network-FS safe) but commits zero the journal header in place instead
# >   of creating+deleting the journal file — directory-metadata operations
# >   that dominate commit latency on NFS/Lustre; journal_size_limit keeps the
# >   persistent journal from staying at its high-water mark.
# > - synchronous=NORMAL: identical to FULL under an *app* crash; only an OS
# >   crash / power loss can lose tail transactions.  Both databases are
# >   recoverable: `ExeData` on disk is the source of truth for job state
# >   (DBResurrect/DBDoctor rebuild from it) and signals are re-emitted on
# >   resubmission.  Skipping the fsync-per-commit shortens write-lock hold
# >   times — the very contention the busy timeout absorbs.
# > - foreign_keys=ON (job DB): SQLite ships with FK enforcement off, so the
# >   `Job.part_id -> part.id` constraint is decorative without it.  `Part`
# >   rows are never deleted, so this can only reject genuinely orphaned rows.
_CONNECT_PRAGMAS: dict[_DBRole, tuple[str, ...]] = {
    _DBRole.JOB: (
        "PRAGMA journal_mode=PERSIST",
        "PRAGMA journal_size_limit=4194304",
        "PRAGMA synchronous=NORMAL",
        "PRAGMA temp_store=MEMORY",
        "PRAGMA foreign_keys=ON",
    ),
    _DBRole.LOG: (
        "PRAGMA journal_mode=PERSIST",
        "PRAGMA journal_size_limit=4194304",
        "PRAGMA synchronous=NORMAL",
        "PRAGMA temp_store=MEMORY",
    ),
}


def _apply_pragmas(pragmas: tuple[str, ...], dbapi_conn, _record) -> None:
    cursor = dbapi_conn.cursor()
    try:
        for pragma in pragmas:
            cursor.execute(pragma)
    finally:
        cursor.close()


# > engine cache keyed by (url, role): DBTasks open sessions extremely often
# > (every `complete()` check the scheduler makes), and building a fresh Engine
# > each time re-opens the SQLite files — the dominant cost on shared/network
# > filesystems.
# >
# > Fork safety: Luigi runs every task attempt in its own `TaskProcess`
# > (multiprocessing).  Under the "spawn" start method (macOS default) the child
# > re-imports this module and starts with an empty cache.  Under "fork" (Linux
# > default) the child inherits the cache *including open pooled connections*,
# > which must never be used across the fork boundary: the `register_at_fork`
# > hook below implements the SQLAlchemy multiprocessing recipe — drop the pool
# > references in the child without closing the inherited descriptors
# > (`dispose(close=False)`, so the parent is unaffected) and let the child
# > build fresh engines/connections on first use.
_ENGINE_CACHE: dict[tuple[str, _DBRole], Engine] = {}


def _dispose_engines_after_fork() -> None:
    """Reset the engine cache in a freshly forked child process."""
    for engine in _ENGINE_CACHE.values():
        engine.dispose(close=False)
    _ENGINE_CACHE.clear()


if hasattr(os, "register_at_fork"):  # POSIX only; Luigi workers fork on Linux
    os.register_at_fork(after_in_child=_dispose_engines_after_fork)


def _cached_engine(url: str, role: _DBRole) -> Engine:
    """Return a per-process cached SQLAlchemy engine for `url` in `role`."""
    key = (url, role)
    engine = _ENGINE_CACHE.get(key)
    if engine is None:
        # > check_same_thread=False: pooled connections may be checked out by a
        # > different thread than the one that created them; the pool serializes
        # > access so the sqlite3 objects are never used concurrently.
        engine = create_engine(
            url, connect_args={"timeout": _SQLITE_TIMEOUT, "check_same_thread": False}
        )
        event.listen(engine, "connect", partial(_apply_pragmas, _CONNECT_PRAGMAS[role]))
        _ENGINE_CACHE[key] = engine
    return engine


class DBTask(Task, metaclass=ABCMeta):
    """the task class to interact with the database"""

    run_tag: float = luigi.FloatParameter()  # type: ignore[assignment]

    # > database queries should jump the scheduler queue?
    # priority = 1

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # @todo all DBTasks need to be started in the job root path: check?
        # > plain joins, not `_local()`: that would stat+mkdir the run directory on
        # > every task construction, and constructions happen constantly (clone
        # > chains, scheduler dedup); the run directory is guaranteed to exist by
        # > `Config.set_path` / CLI validation long before any DBTask is built
        self.dbname: str = "sqlite:///" + str((self._path / "db.sqlite").absolute())
        self.logname: str = "sqlite:///" + str((self._path / "log.sqlite").absolute())
        # > per-instance (instances can point at different databases), keyed by part id
        self._part_name_cache: dict[int, str] = {}

    # > threadsafety using resource = 1, where read/write needed
    @property
    def resources(self):  # type: ignore
        return super().resources | {"DBTask": 1}

    def _part_name(self, part_id: int, session: Session | None = None) -> str:
        """Return the name of `part_id`, memoized per task instance and part id.

        Pass an active `session` wherever one is at hand — a free lookup when
        the `Part` row is already in its identity map — so methods can prime
        the cache and log-prefix properties never open a nested session.
        Without one, a short session is opened as a lazy fallback: task
        construction must not touch the database (Luigi constructs task
        objects constantly: clone chains, scheduler dedup).
        """
        if part_id not in self._part_name_cache:
            if session is not None:
                self._part_name_cache[part_id] = session.get_one(Part, part_id).name
            else:
                with self.session as own_session:
                    self._part_name_cache[part_id] = own_session.get_one(Part, part_id).name
        return self._part_name_cache[part_id]

    def _engine(self, role: _DBRole) -> Engine:
        """Return the (cached) engine for this task's database in `role`."""
        return _cached_engine(self.dbname if role is _DBRole.JOB else self.logname, role)

    @property
    def session(self) -> Session:
        return Session(
            binds={
                DokanDB: self._engine(_DBRole.JOB),
                DokanLog: self._engine(_DBRole.LOG),
            },
            autoflush=False,
        )

    def _safe_commit(self, session: Session) -> None:
        """Commit `session`; on failure roll back and re-raise loudly.

        Lock contention is handled by the SQLite driver itself: connections are
        created with a 420 s busy timeout, so an `OperationalError("database is
        locked")` surfacing here means the lock persisted for that long.
        Retrying `Session.commit()` at this level cannot work — after a failed
        flush/commit SQLAlchemy raises `PendingRollbackError` until
        `rollback()` is called, and rolling back *discards* the pending changes,
        so a "successful" retry would silently commit nothing.  We therefore
        roll back (to release the connection cleanly) and re-raise; recovery is
        left to Luigi task retries, which re-derive state in idempotent `run()`
        implementations.
        """
        try:
            session.commit()
        except OperationalError as e:
            session.rollback()
            dt_str: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # > do NOT use self._logger here: it commits to the log DB itself
            _console.print(
                f"(c)[dim][{dt_str}][/dim](ERROR): DBTask::_safe_commit failed"
                f" [dim](changes rolled back, NOT committed)[/dim]: {e!r}"
            )
            raise RuntimeError(
                "DBTask::_safe_commit: commit failed; pending changes were rolled back"
            ) from e

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
        # > negative values are signals: always store in database (workflow relies on this)
        if level < 0:
            session.add(Log(level=level, timestamp=time.time(), message=message))
            self._safe_commit(session)
        # > pass through log level & all signals
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

    def _update_job(
        self,
        session: Session,
        exe_data: ExeData,
        jobs: dict[int, Job | None] | None = None,
        *,
        add_missing: bool = False,
        skip_terminated: bool = True,
    ) -> None:
        """Synchronize job rows from an `ExeData["jobs"]` payload.

        Parameters
        ----------
        session : Session
            Active SQLAlchemy session bound to the job database.
        exe_data : ExeData
            Execution metadata source containing per-job output fields.
        jobs : dict[int, Job | None] | None, optional
            Optional mapping of job id to pre-fetched DB row. If omitted,
            all job ids found in `exe_data["jobs"]` are considered and rows
            are loaded lazily.
        add_missing : bool, optional
            If True, create missing DB jobs from ExeData entries.
        skip_terminated : bool, optional
            If True, do not overwrite rows that are already terminated.

        Notes
        -----
        - `job_id` keys are normalized to `int`.
        - A missing/invalid result payload marks non-recovery jobs as FAILED.
        - This method commits once at the end.
        """
        exe_jobs = exe_data.get("jobs", {})
        if not isinstance(exe_jobs, dict):
            self._logger(
                session,
                f"_update_job: invalid ExeData jobs payload type: {type(exe_jobs)!r}",
                LogLevel.WARN,
            )
            return

        _jobs: dict[int, Job | None] = {}
        if jobs:
            for raw_job_id, db_job in jobs.items():
                try:
                    _jobs[int(raw_job_id)] = db_job
                except (TypeError, ValueError):
                    self._logger(
                        session,
                        f"_update_job: invalid job id {raw_job_id!r} in input mapping, skipping",
                        LogLevel.WARN,
                    )
        else:
            for raw_job_id in exe_jobs:
                try:
                    _jobs[int(raw_job_id)] = None
                except (TypeError, ValueError):
                    self._logger(
                        session,
                        f"_update_job: invalid ExeData job id {raw_job_id!r}, skipping",
                        LogLevel.WARN,
                    )

        part_name: str = exe_data.path.parent.name
        part: Part | None = session.scalars(select(Part).where(Part.name == part_name)).first()
        if not part:
            self._logger(
                session,
                f"_update_job: part {part_name!r} not found in DB {exe_data.path}",
                level=LogLevel.WARN,
            )

        for job_id in _jobs:
            job: Job | None = _jobs[job_id]
            if job is None:
                job = session.get(Job, job_id)  # fetch from DB

            job_entry = exe_jobs.get(job_id)

            if skip_terminated and job and job.status in JobStatus.terminated_list():
                continue

            if job_entry is None:
                self._logger(
                    session,
                    f"_update_job: job {job_id} not found in ExeData, skipping",
                    LogLevel.WARN,
                )
                if job is not None:
                    job.status = JobStatus.FAILED
                continue

            if not job:
                if add_missing:
                    if not part:
                        continue
                    if "part_id" in exe_data:
                        # assert exe_data["part_id"] == part.id, f"part_id mismatch for {exe_data.path}"
                        # @note:  independent runs could assign different part ids?
                        pass
                    self._logger(session, f"_update_job: job {job_id} not found, adding new entry")
                    job = Job(
                        part_id=part.id,
                        run_tag=exe_data["timestamp"],
                        status=JobStatus.RECOVER,
                        mode=ExecutionMode(exe_data["mode"]),
                        policy=ExecutionPolicy(exe_data["policy"]),
                        timestamp=exe_data["timestamp"],
                        ncall=exe_data["ncall"],
                        niter=exe_data["niter"],
                        rel_path=str(exe_data.path.relative_to(self._local())),
                        elapsed_time=0.0,
                        seed=job_entry["seed"],
                    )
                    session.add(job)
                else:
                    self._logger(
                        session,
                        f"_update_job: job {job_id} not found, skipping",
                        LogLevel.WARN,
                    )
                    continue

            # > job is set: sanity checks & update entries
            assert part_name == job.part.name, (
                f"part name mismatch for job {job_id}: {part_name!r} vs {job.part.name!r}"
            )
            if job_entry["seed"] != job.seed:
                self._logger(
                    session,
                    f"_update_job: seed mismatch for job {job_id}: "
                    f"{job_entry['seed']} vs {job.seed} ({exe_data.path})",
                    LogLevel.WARN,
                )
                continue

            if "result" in job_entry:
                try:
                    res: float = float(job_entry["result"])
                    err: float = float(job_entry["error"])
                    chi2dof: float = float(job_entry["chi2dof"])
                except (KeyError, TypeError, ValueError):
                    self._logger(
                        session,
                        f"_update_job: invalid result payload for job {job_id}, marking FAILED",
                        LogLevel.WARN,
                    )
                    job.status = JobStatus.FAILED
                    continue

                if not (math.isfinite(res) and math.isfinite(err) and math.isfinite(chi2dof)):
                    job.status = JobStatus.FAILED
                else:
                    job.result = res
                    job.error = err
                    job.chi2dof = chi2dof
                    if "elapsed_time" in job_entry:
                        elapsed: float = float(job_entry["elapsed_time"])
                        if elapsed > 0.0:
                            job.elapsed_time = elapsed
                        else:
                            # > keep DB estimates if runtime metadata is broken
                            pass
                    else:
                        # > premature termination of job:  re-scale by iterations that completed
                        niter_completed: int = len(job_entry.get("iterations", []))
                        scale: float = float(niter_completed) / float(job.niter) if job.niter > 0 else 0.0
                        job.niter = niter_completed
                        job.elapsed_time = scale * job.elapsed_time
                    # > retain the DONE vs. MERGED status
                    if job.status not in JobStatus.success_list():
                        job.status = JobStatus.DONE
            else:
                # > recovery will reinstate the original status after this call
                if job.status != JobStatus.RECOVER:
                    job.status = JobStatus.FAILED

            # @todo: status restoration infra
            # @todo: trigger on change if status is in success_list and report & overwrite.

        self._safe_commit(session)

    def _remainders(self, session: Session) -> tuple[int, float]:
        # > remaining resources available
        alloc_jobs = session.scalars(  # active contains time estimates
            select(Job)
            .join(Part)
            .where(Part.active.is_(True))
            .where(Job.run_tag == self.run_tag)
            .where(Job.mode == ExecutionMode.PRODUCTION)
            .where(Job.status.in_(JobStatus.success_list() + JobStatus.active_list()))
        ).all()
        njobs_alloc: int = len(alloc_jobs)
        njobs_rem: int = self.config["run"]["jobs_max_total"] - njobs_alloc
        t_alloc: float = sum(job.elapsed_time for job in alloc_jobs)
        t_rem: float = self.config["run"]["jobs_max_total"] * self.config["run"]["job_max_runtime"] - t_alloc
        return njobs_rem, t_rem

    # @todo make return a UserDict class with a schema?
    def _distribute_time(self, session: Session, total_t: float) -> dict:
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
            # > runtime estimate based on successful jobs *with usable metadata*:
            # > `elapsed_time == 0` (log without an "Elapsed time" line) or `ntot == 0`
            # > (premature termination rescaled `niter` to 0) would poison the sample
            # > with `tau == 0` and divide by zero downstream — exclude such jobs
            if job.status in JobStatus.success_list():
                if job.elapsed_time > 0.0 and ntot > 0:
                    # >--------
                    # A > previously we weighted the longer jobs with a higher weight
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
                else:
                    self._logger(
                        session,
                        "DBTask::_distribute_time:  skipping job without usable runtime metadata"
                        + f" (elapsed_time={job.elapsed_time}, ntot={ntot}) in {job!r}",
                        LogLevel.WARN,
                    )
            # > extra time allocation from active parts & DONE jobs
            if job.status in [*JobStatus.active_list(), JobStatus.DONE]:
                # > everything that was not yet merged needs to be accounted for
                # > in the error estimation & the distribution of *new* jobs
                cache[job.part_id]["Textra"] += job.elapsed_time
                cache[job.part_id]["nextra"] += ntot
            # @todo maybe we would want to include the failed jobs above
            #  to see if they hit the runtime limit?

        # > parts with only active (no successful) production jobs provide no runtime
        # > estimate (norm == 0 would divide by zero below): skip them this round;
        # > their in-flight jobs will supply the estimate once they complete
        for part_id in [pid for pid, ic in cache.items() if ic["norm"] == 0]:
            self._logger(
                session,
                f"DBTask::_distribute_time: part {part_id} has no successful production job"
                " for the current policy: skipping this round",
                LogLevel.WARN,
            )
            del cache[part_id]

        # > check every active part has an entry; compute the min/max/avg error; accumulate tot result & error
        pt_min_error: float = math.inf
        pt_max_error: float = -math.inf
        pt_avg_error: float = 0.0  # avg error on part to get target accuracy
        tot_result: float = 0.0
        tot_error: float = 0.0
        for pt in session.scalars(select(Part).where(Part.active.is_(True))):
            if pt.id not in cache:
                self._logger(
                    session,
                    f"DBTask::_distribute_time: part {pt.id} ({pt.name!r}) has no production jobs"
                    " for the current policy: skipping",
                    LogLevel.WARN,
                )
                continue
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

        # median of errors (inf when no part has a positive error: disables the log damping)
        cached_errors: list[float] = [ic["error"] for ic in cache.values() if ic["error"] > 0.0]
        pt_med_error: float = sorted(cached_errors)[len(cached_errors) // 2] if cached_errors else math.inf

        # > adjusted errors
        # _console.print(cache)
        adj_thresh_min: float = 1.0
        adj_thresh_max: float = 7.0
        adj_penalty: float = 10.0
        t: float = adj_thresh_max * pt_med_error
        for part_id, ic in cache.items():
            ic["adj_error"] = ic["error"]
            # > enforce non-zero errors:  arithmetic mean
            if ic["error"] < adj_thresh_min * pt_min_error:
                ic["adj_error"] = 0.5 * (ic["error"] + adj_thresh_min * pt_min_error)
            # > protect against outliers:  log damping above t
            if ic["error"] > t:
                ic["adj_error"] = t * (1.0 + math.log(ic["error"] / t))
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
        accum_t: float = 0.0
        accum_err_sqrtt: float = 0.0
        while True:
            accum_t = 0.0
            accum_err_sqrtt = 0.0
            for part_id, ic in cache.items():
                if part_id not in result["part"]:
                    i_tau: float = ic["sum"] / ic["norm"]
                    i_tau_err: float = 0.0
                    if ic["count"] > 1:
                        i_tau_err = ic["sum2"] / ic["norm"] - i_tau**2
                        i_tau_err = math.sqrt(i_tau_err) if i_tau_err > 0.0 else abs(i_tau_err)
                    # > convert to time
                    # include estimate from the extra jobs already allocated
                    i_t: float = i_tau * (ic["ntot"] + ic["nextra"])
                    ic["adj_error"] = math.sqrt(
                        ic["adj_error"] ** 2 * ic["ntot"] / (ic["ntot"] + ic["nextra"])
                    )
                    result["part"][part_id] = {
                        "tau": i_tau,
                        "tau_err": i_tau_err,
                        "i_T": i_t,
                        "i_err_sqrtT": ic["adj_error"] * math.sqrt(i_t),
                    }
                # > skip excluded parts
                if result["part"][part_id].get("T_opt", 1.0) > 0.0:
                    accum_t += result["part"][part_id]["i_T"]
                    accum_err_sqrtt += result["part"][part_id]["i_err_sqrtT"]

            # > use E-L formula to compute the optimal distribution of T to the active parts
            # > and flag if parts were removed and we need to recompute
            acc_t_opt: float = 0.0
            no_negative_t_opt: bool = True
            n_excluded_prev: int = sum(1 for ires in result["part"].values() if ires.get("T_opt", 1.0) <= 0.0)
            for _part_id, ires in result["part"].items():
                if ires.get("T_opt", 1.0) <= 0.0:
                    continue
                # i_err_sqrtT: float = ires.pop("i_err_sqrtT")
                i_err_sqrtt: float = ires.get("i_err_sqrtT")
                i_t: float = ires.get("i_T")  # need it for error calc below
                t_opt: float = 0.0
                if accum_err_sqrtt > 0.0:
                    t_opt = (i_err_sqrtt / accum_err_sqrtt) * (total_t + accum_t) - i_t
                if t_opt < 0.0:
                    no_negative_t_opt = False
                    t_opt = 0.0  # flag as excluded from optimization
                ires["T_opt"] = t_opt
                acc_t_opt += t_opt
            n_excluded_curr: int = sum(1 for ires in result["part"].values() if ires.get("T_opt", 1.0) <= 0.0)
            # > check if all T_opt were positive, or no new exclusions (degenerate: avoid infinite loop)
            if no_negative_t_opt or n_excluded_curr == n_excluded_prev:
                self._debug(
                    session,
                    "DBTask::_distribute_time:  skipped: "
                    f"{[pid for pid, ires in result['part'].items() if ires['T_opt'] <= 0.0]}",
                )
                for _, ires in result["part"].items():
                    del ires["i_err_sqrtT"]
                break  # no more negative T_opt (or no progress — degenerate case)

        # > re-normalize at the end for good measure
        # > and compute an estimate for the error to be achieved
        self._debug(session, f"DBTask::_distribute_time:  T={total_t} v.s. {acc_t_opt=}")
        result["tot_result"] = 0.0
        result["tot_error"] = 0.0
        result["tot_adj_error"] = 0.0
        result["tot_error_estimate_opt"] = 0.0
        for part_id, ires in result["part"].items():
            if acc_t_opt > 0:
                ires["T_opt"] *= total_t / acc_t_opt
            i_t: float = ires.get("i_T")
            result["tot_result"] += cache[part_id]["result"]
            result["tot_error"] += cache[part_id]["error"] ** 2
            if math.isnan(cache[part_id]["adj_error"]):
                result["tot_adj_error"] += cache[part_id]["error"] ** 2
            else:
                result["tot_adj_error"] += cache[part_id]["adj_error"] ** 2
            # > `i_t / (i_t + T)` is the variance fraction left after adding time T;
            # > a degenerate part with no invested and no assigned time keeps its error (-> 1.0)
            denom_opt: float = i_t + ires["T_opt"]
            result["tot_error_estimate_opt"] += cache[part_id]["error"] ** 2 * (
                i_t / denom_opt if denom_opt > 0.0 else 1.0
            )
        result["tot_error"] = math.sqrt(result["tot_error"])
        result["tot_adj_error"] = math.sqrt(result["tot_adj_error"])
        result["tot_error_estimate_opt"] = math.sqrt(result["tot_error_estimate_opt"])

        # > use E-L formula to compute a time estimate (beyond T)
        # > needed to achieve the desired accuracy
        target_abs_acc: float = abs(self.config["run"]["target_rel_acc"] * result["tot_result"])
        result["T_target"] = (
            (accum_err_sqrtt / target_abs_acc) ** 2 - accum_t if target_abs_acc > 0.0 else 0.0
        )
        self._debug(
            session,
            f"DBTask::_distribute_time: tot_result = {result['tot_result']}, "
            f"{target_abs_acc=}, T_target={result['T_target']}",
        )
        result["T_target"] = max(0.0, result["T_target"])

        # > split up into jobs
        # (T_max_job, T_job, njobs, ntot_job)
        result["tot_error_estimate_jobs"] = 0.0
        for part_id, ires in result["part"].items():
            # > 10 sigma buffer but never larger than 50% runtime
            # (note that the large buffer reflects that our data sample is biased
            # where termination due to runtime limits either fail or keep estimate)
            tau_buf: float = min(10 * ires["tau_err"], 0.5 * ires["tau"])
            if tau_buf <= 0.0:  # in case we have no clue (tau_err==0): target 50%
                tau_buf = 0.5 * ires["tau"]

            # > target runtime for one job corrected for buffer
            t_max_job: float = self.config["run"]["job_max_runtime"] * (1.0 - tau_buf / ires["tau"])
            if self.config["run"]["job_fill_max_runtime"]:
                njobs: int = round(ires["T_opt"] / t_max_job)
                ntot_job: int = int(t_max_job / ires["tau"])
            else:
                if ires["T_opt"] > 0.0:
                    ntot_min: int = (
                        self.config["production"]["niter"] * self.config["production"]["ncall_start"]
                    )
                    ntot_max: int = int(t_max_job / ires["tau"])
                    njobs: int = int(ires["T_opt"] / t_max_job) + 1
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
            t_job: float = ntot_job * ires["tau"]
            t_jobs: float = njobs * t_job
            ires["T_max_job"] = t_max_job
            ires["T_job"] = t_job
            ires["njobs"] = njobs
            ires["ntot_job"] = ntot_job
            i_t: float = ires.pop("i_T")  # pop it here
            denom_jobs: float = i_t + t_jobs
            result["tot_error_estimate_jobs"] += cache[part_id]["error"] ** 2 * (
                i_t / denom_jobs if denom_jobs > 0.0 else 1.0
            )

        result["tot_error_estimate_jobs"] = math.sqrt(result["tot_error_estimate_jobs"])

        return result
