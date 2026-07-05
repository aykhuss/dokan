"""Dokan Job Runner.

Defines the task to run NNLOJET jobs by spawning executors of the appropriate
backend as specified by the job policy. It is responsible for populating
the database with the results of each execution.
"""

import re
import shutil
from pathlib import Path

import luigi
from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

from ..exe import ExecutionMode, ExecutionPolicy, Executor, ExeData
from ..runcard import RuncardTemplate
from ._dbmerge import MergePart
from ._dbtask import DBTask
from ._jobstatus import JobStatus
from ._sqla import Job


class DBRunner(DBTask):
    """Runner task for executing NNLOJET jobs.

    This task orchestrates the execution lifecycle of a batch of jobs:
    1. Prepares the execution environment (directories, runcards, input files).
    2. Spawns an `Executor` task to run the job(s).
    3. Collects results from `ExeData` and updates the database.
    4. Triggers partial merging if applicable.

    Attributes
    ----------
    ids : list[int]
        List of job IDs to execute in this batch.
    part_id : int
        The ID of the part these jobs belong to.

    """

    _file_run: str = "job.run"

    ids: list[int] = luigi.ListParameter()  # type: ignore[assignment]
    part_id: int = luigi.IntParameter()  # type: ignore[assignment]

    priority = 10

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # > task construction must stay session-free (Luigi constructs task objects
        # > constantly: clone chains, scheduler dedup, dynamic-dependency round-trips);
        # > the DB-derived context is resolved lazily in `_load_context`
        self._context_loaded: bool = False

    @property
    def _logger_prefix(self) -> str:
        # > lazy: the part-name lookup must not happen at construction time
        return self.__class__.__name__ + f"[{self._part_name(self.part_id)}]"

    def _load_context(self, session: Session) -> None:
        """Resolve job/part metadata from the DB (memoized; needed by `run()` only).

        Batch invariants are enforced with explicit exceptions — not `assert`,
        which `python -O` strips: they guard against DB corruption/races and a
        violation must abort the attempt loudly.
        """
        if self._context_loaded:
            return

        jobs: list[Job] = list(session.scalars(select(Job).where(Job.id.in_(self.ids))).all())
        if len(jobs) != len(self.ids):
            missing = sorted(set(self.ids) - {j.id for j in jobs})
            raise RuntimeError(f"DBRunner: job rows vanished from DB: {missing}")
        # > a batch is a single dispatch: uniform part, mode, policy & statistics
        for field in ("part_id", "mode", "policy", "ncall", "niter"):
            if len({getattr(j, field) for j in jobs}) != 1:
                raise RuntimeError(f"DBRunner: mixed {field} in batch {sorted(self.ids)}")
        first: Job = jobs[0]
        if first.part_id != self.part_id:
            raise RuntimeError(f"DBRunner: batch belongs to part {first.part_id}, expected {self.part_id}")
        self.mode: ExecutionMode = ExecutionMode(first.mode)
        self.policy: ExecutionPolicy = ExecutionPolicy(first.policy)
        self.ncall: int = first.ncall
        self.niter: int = first.niter
        if (self.niter * self.ncall) == 0:
            raise RuntimeError(f"job {first.id} has ntot={self.ncall}x{self.niter}==0")
        # > seeds must all be assigned and contiguous: they define the batch directory
        # > name @todo: relax contiguity? (might interfere with resurrection)
        seeds: list[int] = sorted(j.seed for j in jobs if j.seed is not None)
        if len(seeds) != len(jobs) or (seeds[-1] - seeds[0] + 1) != len(jobs):
            raise RuntimeError(f"DBRunner: batch seeds not assigned/contiguous: {seeds}")
        # > assemble job path (also primes the log-prefix cache with the open session)
        self.part_name: str = self._part_name(self.part_id, session)
        self.job_path: Path = self._path.joinpath(
            "raw",
            str(self.mode),
            self.part_name,
            (f"s{seeds[0]}" if seeds[0] == seeds[-1] else f"s{seeds[0]}-{seeds[-1]}"),
        )

        self._context_loaded = True

    def complete(self) -> bool:
        """Check if all jobs in this runner have terminated.

        One aggregate query instead of one lookup per job id: `complete()` is on
        the scheduler's hot path and batches can be `jobs_batch_size` large.
        Job rows that were removed (e.g. `DBRemoveJob`) do not block completion,
        but a row-count mismatch is surfaced as a debug message; `run()` raises
        on missing rows (`_load_context`), so accidental row loss can never
        steer an *executing* runner silently.
        """
        unfinished = case((Job.status.not_in(JobStatus.terminated_list()), 1))
        with self.session as session:
            n_rows, n_unfinished = session.execute(
                select(func.count(Job.id), func.count(unfinished)).where(Job.id.in_(self.ids))
            ).one()
            if n_rows != len(self.ids):
                self._debug(
                    session,
                    self._logger_prefix
                    + f"::complete:  {len(self.ids) - n_rows} job rows missing (removed?)",
                )
        return n_unfinished == 0

    def _prepare_execution(self, exe_data: ExeData) -> None:
        """Prepare the execution directory and ExeData structure.

        DB access is confined to two short sessions (gather inputs; flip the job
        rows to RUNNING); the filesystem work in between — runcard templating
        and copying warmup grids, potentially slow on shared filesystems —
        never holds a DB session.
        """
        # > gather all DB inputs needed for the filesystem work
        with self.session as session:
            self._debug(session, self._logger_prefix + "::run:  prepare execution")
            db_jobs: list[Job] = [session.get_one(Job, job_id) for job_id in self.ids]
            seeds: dict[int, int | None] = {job.id: job.seed for job in db_jobs}
            part_string: str = db_jobs[0].part.string
            part_region: str | None = db_jobs[0].part.region
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
            if LW and not LW.rel_path:
                raise RuntimeError(f"last warmup {LW.id} has no path")
            LW_id: int | None = LW.id if LW else None
            LW_rel_path: str | None = LW.rel_path if LW else None

        # > filesystem work: no DB session held
        # > populate ExeData with all necessary information for the Executor
        exe_data["exe"] = self.config["exe"]["path"]
        exe_data["mode"] = self.mode
        exe_data["policy"] = self.policy
        exe_data["part_id"] = self.part_id

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
        channel_region: str = f"region = {part_region}" if part_region else ""
        template.fill(
            run_file,
            sweep=f"{self.mode!s} = {self.ncall}[{self.niter}]",
            run="",
            channels=part_string,
            channels_region=channel_region,
            toplevel="",
        )
        exe_data["input_files"] = [DBRunner._file_run]

        # > copy grid files
        if LW_rel_path:
            LW_path: Path = self._local(LW_rel_path)
            LW_data: ExeData = ExeData(LW_path)
            if not LW_data.is_final:
                raise RuntimeError(f"last warmup {LW_id} is not final")
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
        exe_data["jobs"] = {job_id: {"seed": seeds[job_id]} for job_id in self.ids}

        # > save to tmp file
        exe_data.write()

        # > flip the job rows to RUNNING now that the execution directory is staged
        with self.session as session:
            rel_path: str = str(self.job_path.relative_to(self._path))
            for job_id in self.ids:
                db_job: Job = session.get_one(Job, job_id)
                db_job.rel_path = rel_path
                db_job.status = JobStatus.RUNNING
            self._safe_commit(session)

    def run(self):  # type: ignore[override]
        """Execute the runner task."""
        # > Phase 1: short DB session: resolve context and the batch status
        with self.session as session:
            self._load_context(session)
            self._logger(session, self._logger_prefix + f"::run:  [dim](job_ids = {self.ids})[/dim]")
            # > DBDispatch takes care to stay within batch size
            db_jobs: list[Job] = [session.get_one(Job, job_id) for job_id in self.ids]
            job_status: JobStatus = JobStatus(db_jobs[0].status)
            if job_status in JobStatus.active_list() and any(j.status != job_status for j in db_jobs):
                raise RuntimeError(
                    self._logger_prefix
                    + f"::run:  mixed statuses in batch: {[(j.id, JobStatus(j.status)) for j in db_jobs]}"
                )

        # > Phase 2: filesystem I/O (job metadata, runcard, warmup grids) and the
        # > executor yield: no DB session held — a session would not survive the
        # > suspend/restart cycle of the dynamic dependency anyway
        exe_data = ExeData(self.job_path)
        if job_status == JobStatus.DISPATCHED and not exe_data.is_final:
            self._prepare_execution(exe_data)

        yield Executor.factory(
            policy=self.policy,
            path=str(self.job_path.absolute()),
            log_level=self.config["ui"]["log_level"],
        )

        # > parse the return data (reached once the executor is complete; a fresh
        # > run() attempt restarts from the top and falls through the yield inline)
        exe_data.load()
        if not exe_data.is_final:
            # > even failed jobs should finalize ExeData
            raise RuntimeError(f"{self.ids} not final?!\n{self.job_path}\n{exe_data.data}")

        # > check if there was an Executor log written out; if yes print it below
        exe_log: Path = exe_data.path / Executor._file_log
        exe_log_lines: list[str] = []
        if exe_log.exists():
            with open(exe_log) as f:
                exe_log_lines = f.readlines()

        # > Phase 3: short DB session: persist results & decide on a re-merge
        mrg_part = None
        with self.session as session:
            if exe_log_lines:
                self._logger(
                    session,
                    self._logger_prefix
                    + f"::run: Executor log [dim]({exe_log})[/dim]:\n"
                    + "\n".join(f" | [dim]{ln.strip()}[/dim]" for ln in exe_log_lines),
                )

            self._update_job(session, exe_data, {int(job_id): None for job_id in self.ids})

            # > see if a re-merge is possible (must come *after* `_update_job`:
            # > the freshly DONE jobs feed MergePart's completeness counts)
            if self.mode == ExecutionMode.PRODUCTION:
                candidate = self.clone(MergePart, force=False, part_id=self.part_id)
                if candidate.complete():
                    self._debug(session, self._logger_prefix + "::run:  MergePart skip")
                else:
                    self._logger(session, self._logger_prefix + "::run:  yield MergePart")
                    mrg_part = candidate
        if mrg_part is not None:
            yield mrg_part
