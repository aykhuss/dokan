"""Dokan job dispatcher task.

`DBDispatch` is responsible for two related actions:
1. Re-populate the queue with new production jobs when needed.
2. Select queued jobs, assign seeds, and hand them over to `DBRunner`.

The task can operate in two modes via `id`:
- `id == 0`: dynamic dispatch (global scheduling logic).
- `id > 0`: dispatch a specific job id.
"""

import time
from collections.abc import Collection

import luigi

# from rich.console import Console
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from dokan.db._loglevel import LogLevel

from .._types import JobSize
from ..distribute import HealthCheck, prescribe_jobs
from ..exe import ExecutionMode
from ._dbmerge import MergeAll
from ._dbproduction import distribute_production_time, production_remainders
from ._dbrunner import DBRunner
from ._dbtask import DBTask
from ._jobstatus import JobStatus
from ._sqla import Job, Log, Part

# _console = Console()


class DBDispatch(DBTask):
    """Queue replenishment and job dispatch coordinator.

    Notes
    -----
    Dynamic dispatch (`id == 0`) is serialized via an extra Luigi resource
    (`DBDispatch`) to avoid concurrent queue mutation by multiple schedulers.
    """

    # > id semantics:
    # >   0  — dynamic global scheduling
    # >  >0  — dispatch a specific Job by its primary key
    id: int = luigi.IntParameter(default=0)  # type: ignore[assignment]

    # > _n distinguishes successive id==0 dispatchers in the chain
    # > (Luigi deduplicates tasks by parameters, so _n must differ per wave)
    _n: int = luigi.IntParameter(default=0)  # type: ignore[assignment]

    # > execution mode and policy are fixed at queue time; dispatch reads them from the DB
    _REPOPULATE_INTERVAL_FAC: float = 0.10
    _SIGNAL_INTERVAL_FAC: float = 0.01
    _DISPATCH_INTERVAL_MIN: float = 10.0
    # > cap the poll intervals: they scale with `job_max_runtime`, and long jobs
    # > (e.g. 24h -> 2.4h re-checks) would otherwise leave the queue idle for hours
    # > while the sleeping dispatcher holds a DBTask resource unit
    _DISPATCH_INTERVAL_MAX: float = 300.0
    _DISPATCH_SIGNAL_ORDER: tuple[LogLevel, ...] = (LogLevel.SIG_MERGE,)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.id < 0:
            # > the "restrict to one part" mode (id < 0) was removed: it never had a caller
            raise ValueError(f"DBDispatch: invalid id = {self.id} (must be 0 or a job id > 0)")
        self._logger_prefix: str = (
            self.__class__.__name__ + f"[{self.id}" + (f",{self._n}" if self.id == 0 else "") + "]"
        )
        self.part_id: int = 0  # set in `_repopulate`

    @property
    def resources(self):  # type: ignore
        """Return Luigi resource locks for this dispatch instance."""
        if self.id == 0:
            return super().resources | {"DBDispatch": 1}
        else:
            return super().resources

    priority = 5  # run dispatchers before lower-priority tasks (default is 0)

    @property
    def select_job(self):
        """Build a base `SELECT Job` query constrained by `run_tag` and `id` mode."""
        # > define the selector for the jobs based on the id that was passed & filter by the run_tag
        slct = select(Job).where(Job.run_tag == self.run_tag)
        if self.id > 0:
            return slct.where(Job.id == self.id)
        return slct

    def _dispatch_settled(self, session: Session) -> bool:
        """Dispatch is complete once stopped and all submitted jobs have drained."""
        if not self._dispatch_done(session):
            return False
        active_remaining = (
            session.scalars(self.select_job.where(Job.status.in_(JobStatus.active_list()))).first()
            is not None
        )
        return not active_remaining

    def _dispatch_done(self, session: Session) -> bool:
        """Dispatch was stopped within the current submission."""
        last_signal = session.scalar(
            select(Log.level)
            .where(Log.level.in_([LogLevel.SIG_SUB, LogLevel.SIG_DISPATCH_DONE]))
            .order_by(Log.id.desc())
            .limit(1)
        )
        return last_signal == LogLevel.SIG_DISPATCH_DONE

    def _stop_dispatch(self, session: Session, reason: str) -> None:
        """Discard all jobs that were never dispatched, then persist the stop.

        If interrupted in between, no stop is recorded and the next pass reaches it again.
        """
        self._discard_queued(session)
        self._logger(session, f"{self._logger_prefix}: {reason}", level=LogLevel.SIG_DISPATCH_DONE)

    def _apply_health(self, session: Session, health: HealthCheck) -> bool:
        """Stop dispatch if production cannot progress, else drop the queues of stuck Parts.

        Returns True if dispatch was stopped.
        """
        if health.is_stalled:
            for message in health.stuck.values():
                self._logger(session, f"{self._logger_prefix}: {message}", level=LogLevel.ERROR)
            self._stop_dispatch(session, "production cannot reach the target accuracy")
            return True
        if health.stuck:
            self._discard_queued(session, health.stuck)
        return False

    def _discard_queued(self, session: Session, part_ids: Collection[int] | None = None) -> None:
        """Delete the QUEUED production jobs (of `part_ids` only, if given)."""
        stmt = delete(Job).where(
            Job.run_tag == self.run_tag, Job.mode == ExecutionMode.PRODUCTION, Job.status == JobStatus.QUEUED
        )
        if part_ids is not None:
            stmt = stmt.where(Job.part_id.in_(part_ids))
        session.execute(stmt)
        self._safe_commit(session)

    def complete(self) -> bool:
        """Dynamic dispatch completes after a stop and all active jobs drain.

        Bounded dispatch completes once its fixed job is no longer queued.
        """
        with self.session as session:
            if self.id == 0:
                done = self._dispatch_settled(session)
                self._debug(session, self._logger_prefix + f"::complete:  {done}")
                return done
            # id != 0: finite dispatch — no QUEUED is sufficient
            if session.scalars(self.select_job.where(Job.status == JobStatus.QUEUED)).first() is not None:
                self._debug(session, self._logger_prefix + "::complete:  False")
                return False
            self._debug(session, self._logger_prefix + "::complete:  True")
        return True

    def _repopulate(self, session: Session) -> bool:
        """Populate the job queue and select the next part to dispatch.

        Return True when the caller should wait; False when it can dispatch
        `part_id` or the submission has stopped.  Before selecting or adding
        jobs, dynamic dispatch discards the queued jobs of stuck Parts and
        stops once no Part can make progress.  Bounded dispatch only looks up
        the part for its fixed job.
        """
        queue_full: bool = False

        if self.id > 0:
            job: Job | None = session.get(Job, self.id)
            if job is None:
                # > the job row was removed (e.g. queued-job purge on resubmission):
                # > nothing to dispatch; leave part_id unset so run() terminates cleanly
                self._logger(
                    session,
                    self._logger_prefix + "::repopulate:  job no longer in DB, nothing to dispatch",
                    level=LogLevel.WARN,
                )
                self.part_id = 0
                return queue_full
            self.part_id = job.part_id
            return queue_full

        self.part_id = 0
        if self._dispatch_done(session):
            return False

        # > check for stuck Parts before any queued job gets selected
        if self._apply_health(session, distribute_production_time(self, session, 0.0).health):
            return False

        # > get the remaining resources but need to go into the loop
        # > to get the correct state of self.part_id
        njobs_rem, T_rem = production_remainders(self, session)

        self._debug(
            session,
            self._logger_prefix + "::repopulate: " + f"njobs = {njobs_rem}, T = {T_rem}",
        )

        def queue_production(part_id: int, njobs: int, job_size: JobSize, T_job: float) -> None:
            """Insert `njobs` QUEUED production jobs of `job_size` (estimated runtime `T_job`)."""
            jobs: list[Job] = [
                Job(
                    run_tag=self.run_tag,
                    part_id=part_id,
                    mode=ExecutionMode.PRODUCTION,
                    policy=self.config["exe"]["policy"],
                    status=JobStatus.QUEUED,
                    timestamp=0.0,
                    ncall=job_size.ncall,
                    niter=job_size.niter,
                    elapsed_time=T_job,
                )
                for _ in range(njobs)
            ]
            session.add_all(jobs)
            self._safe_commit(session)
            self._logger(
                session,
                self._logger_prefix
                + "::repopulate:  "
                + f"register [bold]{njobs}[/bold] jobs for {session.get_one(Part, part_id).name}"
                + f" [dim](job_ids = {[job.id for job in jobs]})[/dim]",
            )

        job_counts = (
            select(
                Job.part_id,
                func.count(Job.id).filter(Job.status == JobStatus.QUEUED).label("nqueued"),
                func.count(Job.id).filter(Job.status.in_(JobStatus.active_list())).label("nactive"),
                func.count(Job.id).filter(Job.status.in_(JobStatus.success_list())).label("nsuccess"),
                func.min(Job.id).filter(Job.status == JobStatus.QUEUED).label("first_queued"),
            )
            .where(Job.run_tag == self.run_tag, Job.mode == ExecutionMode.PRODUCTION)
            .group_by(Job.part_id)
            .subquery()
        )
        part_counts = (
            select(Part, job_counts.c.nqueued, job_counts.c.nactive, job_counts.c.nsuccess)
            .outerjoin(job_counts, Part.id == job_counts.c.part_id)
            .where(Part.active.is_(True))
            .order_by(job_counts.c.first_queued)
        )

        # > populate until some termination condition is reached
        while True:
            no_new_jobs: bool = njobs_rem <= 0 or T_rem <= 0.0

            self.part_id = 0  # reset in each loop, only set when a part is selected for dispatch

            sorted_parts = session.execute(part_counts).all()

            # > (active - queued) = DISPATCHED + RUNNING: the truly in-flight jobs
            tot_inflight: int = 0
            # > oldest part (FIFO by min queued job id) that still has queued jobs;
            # > used as a fallback to drain a sub-batch remainder once budget is exhausted
            drain_part_id: int = 0
            for pt, nque, nact, nsuc in sorted_parts:
                nque = nque or 0
                nact = nact or 0
                nsuc = nsuc or 0
                if nque > 0 and drain_part_id <= 0:
                    drain_part_id = pt.id
                tot_inflight += nact - nque
                # > select the oldest queue with a full batch or enough jobs to double the Part's work
                if (
                    self.part_id <= 0
                    and nque >= self.config["run"]["jobs_batch_unit_size"]
                    and (
                        nque >= self.config["run"]["jobs_batch_size"]
                        or nque >= 2 * (nsuc + nact - nque)
                    )
                ):
                    self.part_id = pt.id
            # > hold repopulation when slots are saturated and a queue buffer is already in place
            max_concurrent: int = self.config["run"]["jobs_max_concurrent"]
            if tot_inflight >= 1.1 * max_concurrent:  # add 10% buffer
                self._logger(
                    session,
                    self._logger_prefix
                    + "::repopulate:  "
                    + f"{tot_inflight} in-flight v.s. {max_concurrent} max -> throttled",
                )
                self.part_id = 0
                return True
            if queue_full:
                # > estimate accuracy reached: done adding jobs, dispatch what is already queued
                return True  # pause repopulation
            # > break when the queue is full enough to dispatch, or budget is exhausted
            if self.part_id > 0 or no_new_jobs:
                # > budget exhausted: dispatch a remaining sub-batch as well, otherwise
                # > these QUEUED jobs would keep the dispatch from ever completing
                if self.part_id <= 0 and no_new_jobs and drain_part_id > 0:
                    self.part_id = drain_part_id
                if self.part_id > 0:
                    pt: Part = session.get_one(Part, self.part_id)
                    self._logger(
                        session,
                        self._logger_prefix + "::repopulate:  " + f"next:  {pt.name}",
                    )
                elif no_new_jobs:
                    # > budget exhausted and queue fully drained: dispatch is terminal
                    self._stop_dispatch(session, "budget exhausted")
                break

            # > allocate & distribute time for next batch of jobs
            T_next: float = min(njobs_rem * self.config["run"]["job_max_runtime"], T_rem)
            self._debug(
                session,
                self._logger_prefix
                + "::repopulate:  "
                + f"njobs_rem={njobs_rem}, T_rem={T_rem}, T_next={T_next}",
            )
            opt_dist = distribute_production_time(self, session, T_next)
            # > jobs may have failed since the check above: never queue on a stale verdict
            if self._apply_health(session, opt_dist.health):
                return False

            # > interrupt when target accuracy reached
            # @todo does not respect the optimization target yet?
            if opt_dist.rel_acc <= self.config["run"]["target_rel_acc"]:
                self._debug(
                    session,
                    self._logger_prefix
                    + "::repopulate:  "
                    + f"rel_acc = {opt_dist.rel_acc} vs. {self.config['run']['target_rel_acc']}",
                )
                self._stop_dispatch(session, "target accuracy reached")
                break
            if opt_dist.bootstrap_part_ids:
                # > no runtime estimate and nothing in flight that could provide one:
                # > queue a batch of start-size jobs and dispatch it right away
                self.part_id = opt_dist.bootstrap_part_ids[0]
                production: dict = self.config["production"]
                queue_production(
                    self.part_id,
                    min(self.config["run"]["jobs_batch_size"], njobs_rem),
                    JobSize(ncall=production["ncall_start"], niter=production["niter"]),
                    self.config["run"]["job_max_runtime"],
                )
                break
            # @todo: place to inject the staggered merge settings?

            opt_dist = prescribe_jobs(
                opt_dist,
                max_jobs=min(njobs_rem, self.config["run"]["jobs_max_concurrent"]),
                batch_size=self.config["run"]["jobs_batch_size"],
            )
            if opt_dist.njobs_total == 0:
                # > no Part can take a job until its runtime sample comes back:
                # > dispatch a queued remainder if there is one, otherwise wait
                if drain_part_id > 0:
                    self.part_id = drain_part_id
                    break
                return True

            # > register the jobs & update the remaining resources for the next iteration
            # > queue the largest runtime shares first: they are dispatched first
            for part_id, opt in sorted(opt_dist.parts.items(), key=lambda item: item[1].T_opt, reverse=True):
                self._debug(session, f"{part_id}: {opt}")
                if opt.njobs > 0:
                    queue_production(part_id, opt.njobs, opt.job_size, opt.T_job)
                    njobs_rem -= opt.njobs
                    T_rem -= opt.njobs * opt.T_job

            queue_full = opt_dist.estimate_rel_acc_jobs <= self.config["run"]["target_rel_acc"]

        return queue_full

    def _dispatch_interval(self) -> float:
        """Return the throttled queue re-population interval in seconds."""
        return min(
            self._DISPATCH_INTERVAL_MAX,
            max(
                self._DISPATCH_INTERVAL_MIN,
                self._REPOPULATE_INTERVAL_FAC * self.config["run"]["job_max_runtime"],
            ),
        )

    def _signal_interval(self) -> float:
        """Return the workflow signal polling interval in seconds."""
        return min(
            self._DISPATCH_INTERVAL_MAX,
            max(
                self._DISPATCH_INTERVAL_MIN,
                self._SIGNAL_INTERVAL_FAC * self.config["run"]["job_max_runtime"],
            ),
        )

    def _consume_merge_signal(self, session: Session) -> bool:
        """Check for and consume SIG_MERGE log entries.

        Consumed signals are downgraded to INFO to preserve the audit trail.
        """
        signals = list(session.scalars(select(Log).where(Log.level == LogLevel.SIG_MERGE)))
        if not signals:
            return False
        for sig in signals:
            sig.level = LogLevel.INFO
            sig.message = f"[consumed] {sig.message}"
        self._safe_commit(session)
        return True

    def _consume_dispatch_signals(self, session: Session) -> list[luigi.Task]:
        """Consume pending dispatch signals in priority order and return their tasks.

        Only actionable dispatch-request signals belong here.  Status markers such as
        ``SIG_DISPATCH_DONE`` or ``SIG_COMP`` must remain untouched because other
        tasks use them as durable completion state.
        """
        signal_tasks: list[luigi.Task] = []
        for signal in self._DISPATCH_SIGNAL_ORDER:
            if signal == LogLevel.SIG_MERGE and self._consume_merge_signal(session):
                self._logger(session, self._logger_prefix + "::run:  SIG_MERGE \u2192 yielding MergeAll")
                # > force merges newly DONE jobs; no reset_tag \u2014 a user merge signal asks for
                # > an up-to-date result, not a from-scratch re-merge of every observable
                # > (that remains the explicit `finalize` CLI path)
                signal_tasks.append(self.clone(MergeAll, force=True, finalize=True))
        return signal_tasks

    def _with_dispatch_continuation(self, tasks: list[luigi.Task]) -> list[luigi.Task]:
        """Return signal tasks followed by one dynamic dispatch continuation."""
        return [*tasks, self.clone(DBDispatch, id=0, _n=self._n + 1)]

    def _poll_dispatch_signals_until(self, deadline: float) -> list[luigi.Task]:
        """Poll dispatch signals until ``deadline`` or an actionable signal appears."""
        while True:
            with self.session as session:
                signal_tasks = self._consume_dispatch_signals(session)
            if signal_tasks:
                return signal_tasks

            remaining = deadline - time.monotonic()
            if remaining <= 0.0:
                return []
            time.sleep(min(self._signal_interval(), remaining))

    def run(self):  # type: ignore[override]
        """Dispatch batches of queued jobs by spawning `DBRunner`s.

        Loops over `_repopulate` until no more parts need dispatching, collecting
        one `DBRunner` per part per iteration.  For dynamic dispatch (`id == 0`)
        all collected runners are yielded together with the *next* dispatcher in
        the chain so that the next wave starts while the current runners are still
        in flight — keeping total in-flight jobs close to `jobs_max_concurrent` at
        all times.  The chain terminates when `_repopulate` writes a
        `SIG_DISPATCH_DONE` log entry.
        """
        queue_full: bool = True
        while queue_full:
            with self.session as session:
                queue_full = self._repopulate(session)
            if queue_full:
                # > only dynamic dispatch (id == 0) can be throttled: bounded dispatch
                # > (`id > 0`) returns from `_repopulate` immediately with False
                signal_tasks = self._poll_dispatch_signals_until(time.monotonic() + self._dispatch_interval())
                if signal_tasks:
                    yield self._with_dispatch_continuation(signal_tasks)
                    return

        runners: list[DBRunner] = []
        done: bool = False
        with self.session as session:
            self._debug(session, self._logger_prefix + "::run:  " + f"part_id = {self.part_id}")
            while True:
                _ = self._repopulate(session)

                # > repopulate returned without selecting a part: nothing left to
                # > dispatch this wave.  Terminal only when `_dispatch_settled` holds
                # > (signal written *and* no active jobs).  If jobs are still in flight
                # > we keep the chain alive to poll them to completion — stopping early
                # > would leave `complete()` False and make Entry busy-loop re-yielding
                # > a non-progressing dispatcher.
                if self.part_id <= 0:
                    done = self._dispatch_settled(session)
                    break

                # > get the queue
                stmt = self.select_job.where(Job.status == JobStatus.QUEUED)
                if self.id == 0:
                    stmt = stmt.where(Job.part_id == self.part_id)
                # > compile batch in `id` order
                jobs: list[Job] = [*session.scalars(stmt.order_by(Job.id.asc())).all()]
                if jobs:
                    # > most recent entry [-1] sets overall statistics
                    for j in jobs:
                        j.ncall = jobs[-1].ncall
                        j.niter = jobs[-1].niter
                        j.elapsed_time = jobs[-1].elapsed_time
                    if self.id == 0:  # only for production dispatch @todo think about warmup & pre-production
                        # > try to exhaust the batch with multiples of the batch unit size;
                        # > fall back to the partial remainder so we never drop queued jobs
                        nbatch_curr: int = min(len(jobs), self.config["run"]["jobs_batch_size"])
                        nbatch_unit: int = self.config["run"]["jobs_batch_unit_size"]
                        nbatch: int = (nbatch_curr // nbatch_unit) * nbatch_unit
                        if nbatch == 0:
                            nbatch = nbatch_curr  # dispatch the partial batch rather than stalling
                        jobs = jobs[:nbatch]

                # > set seeds for the jobs to prepare for a dispatch
                if jobs:
                    # > shield against *any* seed already assigned for this (part, mode) —
                    # > including resurrected batches ahead of the current range and jobs from
                    # > submissions with a different seed_offset — by starting past the global
                    # > maximum.  The unique index on (part_id, mode, seed) backstops the rare
                    # > cross-process race with a loud IntegrityError instead of silent reuse.
                    last_job = session.scalars(
                        select(Job)
                        .where(Job.part_id == self.part_id)
                        .where(Job.mode == jobs[0].mode)
                        .where(Job.seed.is_not(None))
                        .order_by(Job.seed.desc())
                    ).first()
                    seed_start: int = self.config["run"]["seed_offset"] + 1
                    if last_job and last_job.seed and last_job.seed >= seed_start:
                        self._debug(
                            session,
                            self._logger_prefix + "::run:  " + f"{self.id} last job:  {last_job!r}",
                        )
                        seed_start = last_job.seed + 1

                    for iseed, job in enumerate(jobs, seed_start):
                        job.seed = iseed
                        job.status = JobStatus.DISPATCHED
                    self._safe_commit(session)

                    # > collect Runner for this part
                    pt: Part = session.get_one(Part, self.part_id)
                    self._logger(
                        session,
                        self._logger_prefix
                        + "::run:  "
                        + f"submitting {pt.name} jobs with "
                        + (
                            f"seeds: {jobs[0].seed}-{jobs[-1].seed}"
                            if len(jobs) > 1
                            else f"seed: {jobs[0].seed}"
                        ),
                    )
                    runners.append(
                        self.clone(cls=DBRunner, ids=[job.id for job in jobs], part_id=self.part_id)  # type: ignore[arg-type]
                    )
                else:
                    # > repopulate selected a part but no jobs were found: stop
                    break
            self._logger(session, self._logger_prefix + "::run:  " + f"yield {len(runners)} DBRunner")

        # > for dynamic dispatch: yield runners alongside the next dispatcher so
        # > the next wave starts while current runners are still in flight
        next_tasks: list = list(runners)
        if self.id == 0:
            with self.session as session:
                signal_tasks = self._consume_dispatch_signals(session)
            if signal_tasks:
                next_tasks.extend(self._with_dispatch_continuation(signal_tasks))
            elif not done:
                # > nothing new to dispatch but the workflow is not finished yet
                # > (active jobs still draining after the dispatch-done signal): pace
                # > the continuation so the chain polls instead of busy-spinning.
                if not runners:
                    time.sleep(self._signal_interval())
                next_tasks.append(self.clone(DBDispatch, id=0, _n=self._n + 1))
        if next_tasks:
            yield next_tasks
