"""Workflow entry task orchestrating pre-production, dispatch, and merging."""

import time

import luigi
from sqlalchemy import select

from .db import DBTask, MergeAll, Part
from .db._dbdispatch import DBDispatch
from .db._dbmerge import MergeFinal
from .db._dbresurrect import DBResurrect
from .db._loglevel import LogLevel
from .db._sqla import Job, Log
from .exe._exe_config import ExecutionMode
from .preproduction import PreProduction


class Entry(DBTask):
    """Root Luigi task for one Dokan submission cycle.

    The task coordinates:
    1. pre-production (including optional warmup resurrection),
    2. production dispatch (including optional production resurrection),
    3. final merge and completion signaling.
    """

    resurrect_jobs: dict = luigi.DictParameter(default={})

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger_prefix: str = self.__class__.__name__
        self._resurrect_jobs: dict[int, dict] = {
            int(job_id): job_entry for job_id, job_entry in self.resurrect_jobs.items()
        }
        with self.session as session:
            self._debug(session, f"{self._logger_prefix}::init {time.ctime(self.run_tag)}")

    def requires(self):
        """Entry has no external Luigi prerequisites."""
        return []

    def output(self):
        """Completion is tracked in DB logs, not filesystem targets."""
        return []

    def complete(self) -> bool:
        """Return True when a completion signal log entry is present."""
        with self.session as session:
            last_log = session.scalars(select(Log).order_by(Log.id.desc())).first()
            if last_log and last_log.level in [LogLevel.SIG_COMP]:
                return True
        return False

    def _rebind_run_tag(self, session) -> None:
        """Move resurrected warmup jobs onto the current run tag.

        We don't do this for production to not count them towards the task limits
        """
        if not self._resurrect_jobs:
            return
        for job_id, job_entry in self._resurrect_jobs.items():
            if ExecutionMode(job_entry["mode"]) != ExecutionMode.WARMUP:
                continue
            job: Job | None = session.get(Job, job_id)
            if not job:
                self._logger(
                    session,
                    f"{self._logger_prefix}::run: job {job_id} not found for warmup resurrection",
                    level=LogLevel.WARN,
                )
                continue
            if job.run_tag != self.run_tag:
                job.run_tag = self.run_tag
        self._safe_commit(session)

    def run(self):
        """Execute one full workflow iteration."""
        if self.complete():
            return

        # > all pre-productions must complete before we can dispatch production jobs
        preprods: list = []
        with self.session as session:
            self._debug(session, f"{self._logger_prefix}::run")
            self._rebind_run_tag(session)

            for pt in session.scalars(select(Part).where(Part.active.is_(True))):
                preprod = self.clone(
                    cls=PreProduction,
                    part_id=pt.id,
                )
                preprods.append(preprod)
            # > add warmup resurrection tasks
            if self._resurrect_jobs:
                preprods = [
                    self.clone(DBResurrect, rel_path=rp)
                    for rp in {
                        jd["rel_path"]
                        for jd in self._resurrect_jobs.values()
                        if ExecutionMode(jd["mode"]) == ExecutionMode.WARMUP
                    }
                ] + preprods
            self._logger(session, f"{self._logger_prefix}::run:  yield preprods")
            yield preprods

            self._logger(session, f"{self._logger_prefix}::run:  complete preprods -> MergeAll")
            yield self.clone(MergeAll, force=True, reset_tag=self.run_tag)

            self._logger(session, f"{self._logger_prefix}::run:  complete MergeAll -> dispatch")
            n_dispatch: int = max(len(preprods), self.config["run"]["jobs_max_concurrent"])
            dispatch: list = [self.clone(DBDispatch, id=0, _n=n) for n in range(n_dispatch)]
            dispatch[0]._repopulate(session)
            # > add production resurrection tasks
            if self._resurrect_jobs:
                dispatch = [
                    self.clone(DBResurrect, run_tag=r[0], rel_path=r[1])
                    for r in {
                        (jd["run_tag"], jd["rel_path"])
                        for jd in self._resurrect_jobs.values()
                        if ExecutionMode(jd["mode"]) == ExecutionMode.PRODUCTION
                    }
                ] + dispatch
            self._debug(session, f"{self._logger_prefix}::run:  yield dispatch")
            yield dispatch

            self._logger(session, f"{self._logger_prefix}::run:  complete dispatch -> MergeFinal")
            yield self.clone(MergeFinal, force=True)
            # yield self.clone(MergeFinal, force=True, reset_tag=time.time(), grids=True)
            # > should already been triggered in MergeFinal but for good measure
            self._logger(session, f"{self._logger_prefix}::run:  complete", level=LogLevel.SIG_COMP)
