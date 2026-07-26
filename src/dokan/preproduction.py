from dataclasses import dataclass

import luigi
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .db import DBTask, Job, JobStatus
from .db._dbdispatch import DBDispatch
from .db._dbresurrect import DBResurrect
from .db._loglevel import LogLevel
from .exe import ExecutionMode
from .exe._exe_data import ExeData
from .warmup import (
    JobSize,
    PreProductionSettings,
    SizingDecision,
    WarmupAssessmentQC,
    WarmupCompleteQC,
    WarmupJobQC,
    WarmupSettingsQC,
    assess_warmup,
    resize_failed_preproduction,
    size_preproduction,
)


@dataclass(frozen=True)
class JobRef:
    """Reference to a queued or active workflow job."""

    id: int


@dataclass(frozen=True)
class WarmupRestartSeeded:
    """A warmup restart created a new job."""

    job: JobRef


@dataclass(frozen=True)
class WarmupRestartPending:
    """A warmup job already exists and will continue the restart."""

    job: JobRef


WarmupRestartOutcome = WarmupRestartSeeded | WarmupRestartPending | WarmupCompleteQC


class PreProduction(DBTask):
    part_id: int = luigi.IntParameter()  # type: ignore[assignment]

    priority = 150

    @property
    def resources(self):  # type: ignore
        # > each part can only have one active pre-production
        return super().resources | {f"PreProduction_{self.part_id}": 1}

    @property
    def _logger_prefix(self) -> str:
        # > lazy: the part-name lookup must not happen at construction time
        return self.__class__.__name__ + f"[{self._part_name(self.part_id)}]"

    def complete(self) -> bool:
        # > complete <=> the sizing ran on the settled grid (ADR-0001): a
        # > successful pre-production newer than the newest warmup job — of
        # > *any* status — exists.  Any newer warmup row re-opens the phase:
        # > a queued/running one is grid work in flight, a FAILED one is a
        # > restart that `run()` must retry loudly, never silently resume
        # > over with the stale grid.  This check must stay cheap, read-only,
        # > and free of file IO — it runs unserialized in every worker process.
        with self.session as session:
            return self._successful_production(session) is not None

    def _newest_warmup_stmt(self):
        """Select the id of the newest warmup job of any status — the last word
        on the grid.

        Jobs are queued in strict phase order per part, so production jobs
        with smaller ids predate this grid (stale after a warmup restart),
        and a non-successful newest warmup means the grid is not settled.
        """
        return (
            select(func.max(Job.id))
            .where(Job.part_id == self.part_id)
            .where(Job.mode == ExecutionMode.WARMUP)
        )

    def _newest_warmup_id(self, session: Session) -> int:
        return session.scalar(self._newest_warmup_stmt()) or 0

    def _successful_production(self, session: Session) -> int | None:
        # > only count productions sized on the settled grid: a warmup restart
        # > makes older pre-productions stale and re-opens the phase
        # > (single statement: this backs `complete()`, the scheduler hot path)
        return session.scalars(
            select(Job.id)
            .where(Job.part_id == self.part_id)
            .where(Job.mode == ExecutionMode.PRODUCTION)
            .where(Job.policy == self.config["exe"]["policy"])
            .where(Job.status.in_(JobStatus.success_list()))
            .where(Job.id > func.coalesce(self._newest_warmup_stmt().scalar_subquery(), 0))
            .order_by(Job.id.asc())
        ).first()

    def _queue_job(self, session: Session, mode: ExecutionMode, size: JobSize) -> JobRef:
        new_job = Job(
            run_tag=self.run_tag,
            part_id=self.part_id,
            mode=mode,
            policy=self.config["exe"]["policy"],
            status=JobStatus.QUEUED,
            timestamp=0.0,
            ncall=size.ncall,
            niter=size.niter,
        )
        session.add(new_job)
        self._safe_commit(session)
        return JobRef(new_job.id)

    def _iteration_errors(self, job: Job) -> list[float]:
        # > QC measures that require the ExeData information; a job without parsed
        # > iterations (or with missing/partial metadata) gives no basis to assess
        # > error stability: empty list leaves CONST_ERR unset
        exe_data: ExeData = ExeData(self._local(job.rel_path))
        job_data: dict = exe_data.get("jobs", {}).get(job.id, {})
        return [it["error"] for it in job_data.get("iterations", [])]

    def _active_job_id(
        self,
        session: Session,
        mode: ExecutionMode,
        *,
        current_submission_only: bool = True,
    ) -> int | None:
        """Oldest active job for `mode`, optionally limited to this submission.

        Productions are policy-scoped; warmups are not (grids are shared
        across execution policies).
        """
        query = (
            select(Job.id)
            .where(Job.part_id == self.part_id)
            .where(Job.mode == mode)
            .where(Job.status.in_(JobStatus.active_list()))
            .order_by(Job.id.asc())
        )
        if current_submission_only:
            query = query.where(Job.run_tag == self.run_tag)
        if mode is ExecutionMode.PRODUCTION:
            query = query.where(Job.policy == self.config["exe"]["policy"])
        return session.scalars(query).first()

    def _successful_warmups(self, session: Session) -> list[Job]:
        """Successful warmups, most recent first, without degenerate rows.

        Premature terminations can land DONE with `niter` rescaled to 0 (no
        statistics): no basis for QC or sizing, and `ntot` is a divisor in
        the assessment — exclude them (cf. the same guard in
        `_distribute_time`).
        """
        past_warmups = session.scalars(
            select(Job)
            .where(Job.part_id == self.part_id)
            .where(Job.mode == ExecutionMode.WARMUP)
            .where(Job.status.in_(JobStatus.success_list()))
            .order_by(Job.id.desc())
        ).all()
        return [job for job in past_warmups if job.ncall and job.niter]

    def _assess_warmup(self, session: Session) -> WarmupAssessmentQC:
        """Gather persisted warmup data and evaluate the pure QC decision."""
        past_warmups: list[Job] = self._successful_warmups(session)
        return assess_warmup(
            past=[WarmupJobQC.from_row(job) for job in past_warmups],
            iteration_errors=lambda: self._iteration_errors(past_warmups[0]),
            settings=WarmupSettingsQC.from_config(self.config),
        )

    def _warmup_step(self, session: Session) -> JobRef | WarmupCompleteQC:
        """Advance normal warmup execution to pending work or completion."""
        active_warmup: int | None = self._active_job_id(session, ExecutionMode.WARMUP)
        if active_warmup is not None:
            return JobRef(active_warmup)

        assessment = self._assess_warmup(session)
        if isinstance(assessment, WarmupCompleteQC):
            return assessment
        return self._queue_job(session, ExecutionMode.WARMUP, assessment.size)

    def seed_warmup_restart(self) -> WarmupRestartOutcome:
        """Seed or adopt a warmup restart for this part."""
        with self.session as session:
            active_warmup = self._active_job_id(
                session,
                ExecutionMode.WARMUP,
                current_submission_only=False,
            )
            if active_warmup is not None:
                return WarmupRestartPending(JobRef(active_warmup))

            assessment = self._assess_warmup(session)
            if isinstance(assessment, WarmupCompleteQC):
                return assessment
            return WarmupRestartSeeded(self._queue_job(session, ExecutionMode.WARMUP, assessment.size))

    def _production_step(self, session: Session) -> JobRef | None:
        """Advance pre-production to pending work, or return None once successful."""
        # > if there's one complete, we're not in pre-production stage!
        if self._successful_production(session) is not None:
            return None

        active_production: int | None = self._active_job_id(session, ExecutionMode.PRODUCTION)
        if active_production is not None:
            return JobRef(active_production)

        settings: PreProductionSettings = PreProductionSettings.from_config(self.config)

        # > terminated on the current grid but not successful => failed
        # > pre-production (pre-restart productions must not enter the retry
        # > sizing: they were sized on a stale grid)
        newest_warmup_id: int = self._newest_warmup_id(session)
        failed_production = session.scalars(
            select(Job)
            .where(Job.part_id == self.part_id)
            .where(Job.mode == ExecutionMode.PRODUCTION)
            .where(Job.policy == self.config["exe"]["policy"])
            .where(Job.status.in_(JobStatus.terminated_list()))
            .where(Job.id > newest_warmup_id)
            .order_by(Job.id.desc())
        ).first()
        if failed_production:
            sizing: SizingDecision = resize_failed_preproduction(
                JobSize(ncall=failed_production.ncall, niter=failed_production.niter), settings
            )
        else:
            # > size the pre-production (PP) with time estimates from the
            # > highest-statistics warmup job we got
            past_warmups: list[Job] = self._successful_warmups(session)
            if not past_warmups:
                raise RuntimeError(f"pre-production: no warmup found for {self.part_id}")
            sizing = size_preproduction(WarmupJobQC.from_row(past_warmups[0]), settings)

        if sizing.warning:
            self._logger(session, sizing.warning, level=LogLevel.WARN)
        return self._queue_job(session, ExecutionMode.PRODUCTION, sizing.size)

    def _dispatch_then_resurrect(self, job: JobRef, stage: str):
        """Yield the bounded dispatch of `job_id`, then a resurrection when reached inline.

        Luigi only continues past a dynamic `yield` in the same pass when the
        yielded task was already complete at yield time; for a bounded dispatch
        that means `job_id` is no longer QUEUED, i.e. an already-active job from
        a previous run that must be resurrected.  Its `rel_path` is re-read
        *after* the yield: dispatch completion only guarantees DISPATCHED, and
        a concurrent `DBRunner` may assign the path at any moment — a job that
        still has none cannot be resurrected and is a loud error.
        """
        yield self.clone(cls=DBDispatch, id=job.id)
        with self.session as session:
            self._logger(
                session,
                self._logger_prefix + f"::run:  resurrect {stage} [dim](job_id = {job.id})[/dim]",
            )
            rel_path: str | None = session.get_one(Job, job.id).rel_path
        if rel_path is None:
            raise RuntimeError(self._logger_prefix + f"::run:  job {job.id} has no path to resurrect")
        yield self.clone(cls=DBResurrect, rel_path=rel_path)

    def run(self):  # type: ignore[override]
        """Drive the warmup/pre-production state machine.

        Every `yield` sits outside a DB session: Luigi abandons the generator on
        suspension (the `with` block would never exit and leak the session).
        """
        # > warmup stage
        with self.session as session:
            self._part_name(self.part_id, session)  # prime the log-prefix cache
            self._logger(session, self._logger_prefix + "::run")
            step: JobRef | WarmupCompleteQC = self._warmup_step(session)
            if isinstance(step, JobRef):
                self._logger(
                    session, self._logger_prefix + f"::run:  yield warmup [dim](job_id = {step.id})[/dim]"
                )
        if isinstance(step, JobRef):
            yield from self._dispatch_then_resurrect(step, "warmup")
        assert isinstance(step, WarmupCompleteQC), (
            self._logger_prefix + f'::run:  warmup step = {step}: "done" decision expected!'
        )

        # > pre-production stage
        with self.session as session:
            self._logger(
                session,
                self._logger_prefix + "::run:  warmup done" + f" [dim]{step.flags}[/dim]",
            )
            job: JobRef | None = self._production_step(session)
            if job is not None:
                self._logger(
                    session,
                    self._logger_prefix + f"::run:  yield pre-production [dim](job_id = {job.id})[/dim]",
                )
        if job is not None:
            yield from self._dispatch_then_resurrect(job, "pre-production")
