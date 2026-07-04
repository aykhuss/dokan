"""Workflow entry task orchestrating pre-production, dispatch, and merging."""

import hashlib
import json
from pathlib import Path

import luigi
from sqlalchemy import select

from .db import DBTask, MergeAll, Part
from .db._dbdispatch import DBDispatch
from .db._dbmerge import MergeFinal
from .db._dbresurrect import DBResurrect
from .db._loglevel import LogLevel
from .db._sqla import Job, Log
from .exe._exe_config import ExecutionMode
from .merge._core import merge_settings
from .preproduction import PreProduction
from .util import is_finite_number, read_json_sidecar, write_json_sidecar


def merge_config_digest(config: dict) -> str:
    """Digest over everything that must trigger a global re-merge when it changes.

    Covers the merge-algorithm settings (shared with the `MergeObs` sidecar identity
    via `merge_settings`) and the set of observables (a newly added histogram needs a
    one-time full merge to produce its part-level `.dat` files).
    """
    payload = {
        "merge": merge_settings(config),
        "histograms": sorted(config["run"]["histograms"]),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def merge_config_reset_tag(config: dict, marker_path: Path, run_tag: float) -> float:
    """Timestamp of the last merge-config change, persisted at `marker_path`.

    Passed as `reset_tag` to the per-submit `MergeAll` so that a resubmit with
    unchanged merge settings does not invalidate every already-merged observable
    (`force=True` still merges newly DONE jobs), while a real settings change forces
    a full re-merge through the existing mechanism: `MergePart.complete()`
    short-circuits on `pt.timestamp < reset_tag` and the `MergeObs` sidecars compare
    the same tag.  Self-healing: the tag only advances when the digest changes, so a
    crash mid-invalidation keeps re-issuing the same tag until every part has been
    re-merged past it.  Idempotent across Luigi's run() restarts for the same reason
    (re-entering `Entry.run()` yields the same `MergeAll` task id).
    """
    digest = merge_config_digest(config)
    marker = read_json_sidecar(marker_path)
    if (
        isinstance(marker, dict)
        and marker.get("digest") == digest
        and is_finite_number(marker.get("changed_at"))
    ):
        return float(marker["changed_at"])
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    write_json_sidecar(marker_path, {"digest": digest, "changed_at": float(run_tag)})
    return float(run_tag)


class Entry(DBTask):
    """Root Luigi task for one Dokan submission cycle.

    The task coordinates:
    1. pre-production (including optional warmup resurrection),
    2. production dispatch (including optional production resurrection),
    3. final merge and completion signaling.
    """

    resurrect_jobs: dict = luigi.DictParameter(default={})  # type: ignore[assignment]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger_prefix: str = self.__class__.__name__
        self._resurrect_jobs: dict[int, dict] = {
            int(job_id): job_entry for job_id, job_entry in self.resurrect_jobs.items()
        }

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

        Production resurrection jobs are deliberately left on their original
        run tag so they do not count against the current submission's concurrency
        and total-job limits.
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

    def run(self):  # type: ignore[override]
        """Execute one full workflow iteration.

        Every `yield` sits outside a DB session: Luigi abandons the generator on
        suspension (the `with` block would never exit and leak the session), and
        each resumption restarts `run()` from the top with fresh sessions anyway.
        """
        if self.complete():
            return

        # > stage 1: all pre-productions must complete before we can dispatch production jobs
        with self.session as session:
            self._debug(session, f"{self._logger_prefix}::run")
            self._rebind_run_tag(session)

            preprods: list = [
                self.clone(cls=PreProduction, part_id=pt.id)
                for pt in session.scalars(select(Part).where(Part.active.is_(True)))
            ]
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

        # > stage 2: merge all pre-production results.  `reset_tag` is the timestamp of
        # > the last merge-config change, *not* the current run tag: a resubmit with
        # > unchanged settings must not re-merge every observable from scratch
        # > (force=True still picks up any newly DONE jobs).
        reset_tag: float = merge_config_reset_tag(
            self.config, self._path / "result" / "merge-config.json", self.run_tag
        )
        with self.session as session:
            self._logger(session, f"{self._logger_prefix}::run:  complete preprods -> MergeAll")
        yield self.clone(MergeAll, force=True, reset_tag=reset_tag)

        # > stage 3: production dispatch (+ production resurrection)
        with self.session as session:
            self._logger(session, f"{self._logger_prefix}::run:  complete MergeAll -> dispatch")
            dispatch_task = self.clone(DBDispatch, id=0, _n=0)
            _ = dispatch_task._repopulate(session)  # type: ignore[attr-defined]
            dispatch: list = [] if dispatch_task.complete() else [dispatch_task]
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
            if dispatch:
                self._debug(session, f"{self._logger_prefix}::run:  yield dispatch")
        if dispatch:
            yield dispatch

        # > stage 4: final merge & completion signal
        with self.session as session:
            self._logger(session, f"{self._logger_prefix}::run:  complete dispatch -> MergeFinal")
        yield self.clone(MergeFinal, force=True)
        # yield self.clone(MergeFinal, force=True, reset_tag=time.time(), grids=True)
        # > SIG_COMP may already be written inside MergeFinal; log it here explicitly
        # > so that Entry.complete() is satisfied even if MergeFinal skips it
        with self.session as session:
            self._logger(session, f"{self._logger_prefix}::run:  complete", level=LogLevel.SIG_COMP)
