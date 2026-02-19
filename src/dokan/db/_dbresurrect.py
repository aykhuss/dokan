"""Dokan Job Resurrection.

Defines a task attempting to resurrect a job that is in a `RUNNING` state
from an old run. A previous run might have been cancelled or failed due
to the loss of a ssh connection or process termination.
"""

import math

import luigi

from dokan.db._loglevel import LogLevel

from ..exe import Executor, ExeData
from ._dbtask import DBTask
from ._jobstatus import JobStatus
from ._sqla import Job


class DBResurrect(DBTask):
    """Task to resurrect and recover a running job.

    This task re-attaches to an existing job directory.
    If `only_recover` is False (default), it spawns an `Executor` to ensure
    completion.
    If `only_recover` is True, it passively scans the directory to update
    the database status without triggering execution.

    Attributes
    ----------
    rel_path : str
        Relative path to the job execution directory.
    only_recover : bool
        If True, only scan for results without re-executing (default: False).

    """

    rel_path: str = luigi.Parameter()
    only_recover: bool = luigi.BoolParameter(default=False)

    priority = 200

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # > pick up from where we left off
        self.exe_data: ExeData = ExeData(self._local(self.rel_path))
        self._logger_prefix: str = self.__class__.__name__

    def requires(self):
        """Return dependencies needed to complete the resurrection flow.

        In active mode (`only_recover=False`) we require the backend `Executor`
        task so that unfinished jobs can continue. In passive mode the task
        only scans the existing job directory and therefore has no dependencies.
        """
        if self.only_recover:
            return []

        if "policy" not in self.exe_data:
            raise RuntimeError(f"{self._logger_prefix}::requires: missing execution policy in {self.exe_data.path}")

        with self.session as session:
            self._debug(session, f"{self._logger_prefix}::requires:  rel_path = {self.rel_path}")
        return [
            Executor.factory(
                policy=self.exe_data["policy"],
                path=str(self.exe_data.path.absolute()),
            )
        ]

    def complete(self) -> bool:
        """Check if this resurrection task has reached a stable DB state.

        Active mode returns True only when all jobs referenced by `ExeData` are
        in terminated states. Passive mode returns True once no referenced job
        remains in `RECOVER`, allowing dispatch/doctor tasks to continue.
        """
        with self.session as session:
            self._debug(session, f"{self._logger_prefix}::complete: {self.rel_path}")
            for job_id in self.exe_data["jobs"]:
                job: Job | None = session.get(Job, job_id)

                if self.only_recover:
                    # > recovery:  clear out all RECOVER status jobs
                    if not job:
                        continue
                    if job.status == JobStatus.RECOVER:
                        return False
                else:
                    # > resurrection:  not terminated, we are not complete.
                    if not job:
                        return False
                    if job.status not in JobStatus.terminated_list():
                        return False

        return True

    def _is_valid_result(self, result: float, error: float) -> bool:
        """Return True when result/error are finite NNLOJET outputs."""
        return math.isfinite(result) and math.isfinite(error)

    def _update_job_from_entry(self, db_job: Job, job_entry: dict) -> None:
        """Copy parsed execution values from `ExeData` into a DB row."""
        db_job.result = float(job_entry["result"])
        db_job.error = float(job_entry["error"])
        db_job.chi2dof = float(job_entry["chi2dof"])
        elapsed: float = float(job_entry["elapsed_time"])
        # > keep DB estimates if runtime metadata is broken/missing
        if elapsed > 0.0:
            db_job.elapsed_time = elapsed

    def run(self):
        """Process resurrection output and update job rows.

        Workflow
        --------
        1. Reload `ExeData` from disk to capture Executor/file-system updates.
        2. In passive mode, scan log/output files to refresh in-memory results.
        3. Update each DB job row according to parsed result availability.

        Status policy
        -------------
        - valid result -> `DONE`
        - invalid numerical result -> `FAILED`
        - missing result in active mode -> `FAILED`
        - missing result in passive mode -> `RUNNING` (let scheduler decide next action)
        """
        # > Re-load to capture changes made by Executor (if any) or filesystem
        self.exe_data.load()

        if self.only_recover:
            # > Passive scan: update ExeData from logs found on disk
            self.exe_data.scan_dir()
        elif not self.exe_data.is_final:
            # > Active mode requires ExeData to be finalized by Executor
            raise RuntimeError(f"Job at {self.rel_path} did not finalize correctly.")

        with self.session as session:
            self._logger(
                session,
                f"{self._logger_prefix}::run:  {self.rel_path}, run_tag = {self.run_tag}",
            )

            for job_id, job_entry in self.exe_data["jobs"].items():
                db_job: Job | None = session.get(Job, job_id)
                if not db_job:
                    self._logger(
                        session,
                        f"Job {job_id} not found in DB during resurrection",
                        level=LogLevel.WARN,
                    )
                    continue

                if "result" in job_entry:
                    res = float(job_entry["result"])
                    err = float(job_entry["error"])
                    if not self._is_valid_result(res, err):
                        db_job.status = JobStatus.FAILED
                    else:
                        self._update_job_from_entry(db_job, job_entry)
                        db_job.status = JobStatus.DONE
                else:
                    # Active mode: missing result implies failure
                    # Passive mode: missing result implies still incomplete
                    db_job.status = JobStatus.FAILED if not self.only_recover else JobStatus.RUNNING

            self._safe_commit(session)
