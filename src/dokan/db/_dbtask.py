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
        engine = create_engine(url, connect_args={"timeout": _SQLITE_TIMEOUT, "check_same_thread": False})
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
            raise RuntimeError("DBTask::_safe_commit: commit failed; pending changes were rolled back") from e

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

    def _flush_logs(self, entries: list[tuple[str, LogLevel]]) -> None:
        """Emit `(message, level)` entries collected during session-free work.

        Companion to the phased `run()` pattern: filesystem/HDF5 work that must
        not hold a DB session defers its log messages and flushes them here in
        one short session afterwards.  No session is opened for an empty list.
        """
        if not entries:
            return
        with self.session as session:
            for message, level in entries:
                self._logger(session, message, level=level)

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
