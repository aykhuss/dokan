"""Dokan Job Resurrection.

Defines a task attempting to resurrect a job that is in an "active" state
from an old run. A previous run might have been cancelled or failed due
to the loss of a ssh connection or process termination.
"""

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
    recover_jobs : dict
        Mapping of job id -> original DB job dictionary captured before setting
        jobs to `RECOVER`. If non-empty, task runs in active recovery mode.

    """

    rel_path: str = luigi.Parameter()
    recover_jobs: dict = luigi.DictParameter(default={})

    priority = 200

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # > pick up from where we left off
        self.exe_data: ExeData = ExeData(self._local(self.rel_path))
        self._logger_prefix: str = self.__class__.__name__ + f"[dim]({self.run_tag})[/dim]"
        self._recover_jobs: dict[int, dict] = {}
        for job_id, payload in self.recover_jobs.items():
            self._recover_jobs[int(job_id)] = payload

        if self._recover_jobs:
            self._logger_prefix += "[dim](recover)[/dim]"
            for job_id, job_entry in self._recover_jobs.items():
                if "status" not in job_entry:
                    raise RuntimeError(f"{self._logger_prefix}::init: missing status for job {job_id}")
                # if JobStatus(job_entry["status"]) == JobStatus.RECOVER:
                #     raise RuntimeError(
                #         f"{self._logger_prefix}::init: recover_jobs[{job_id}] has RECOVER status"
                #     )
                if job_id not in self.exe_data["jobs"]:
                    raise RuntimeError(
                        f"{self._logger_prefix}::init: job {job_id} not found in ExeData at {self.rel_path}"
                    )

    def requires(self):
        """Return dependencies needed to complete the resurrection flow.

        In resurrection mode (no `recover_jobs`) we require the backend
        `Executor` task so unfinished jobs can continue.
        In recovery-only mode (`recover_jobs` provided) the task only scans
        existing outputs and therefore has no dependencies.
        """
        if self._recover_jobs:
            return []

        if "policy" not in self.exe_data:
            raise RuntimeError(
                f"{self._logger_prefix}::requires: missing execution policy in {self.exe_data.path}"
            )

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

        Resurrection mode returns True only when all ExeData jobs are
        terminated. Recovery-only mode returns True once all tracked recovery
        jobs have moved out of `RECOVER`.
        """
        with self.session as session:
            self._debug(session, f"{self._logger_prefix}::complete: {self.rel_path}")
            for job_id in self.exe_data["jobs"]:
                job: Job | None = session.get(Job, job_id)

                if self._recover_jobs:
                    # > recovery-only: only tracked jobs participate in completion
                    if job_id not in self._recover_jobs:
                        continue
                    if not job:
                        self._logger(
                            session,
                            f"Job {job_id} not found in DB during resurrection",
                            level=LogLevel.WARN,
                        )
                    elif job.status == JobStatus.RECOVER:
                        return False
                else:
                    # > resurrection:  not terminated, we are not complete.
                    if not job:
                        self._logger(
                            session,
                            f"Job {job_id} not found in DB during resurrection",
                            level=LogLevel.WARN,
                        )
                    elif job.status not in JobStatus.terminated_list():
                        return False

        return True

    def _all_jobs_terminated(self, session, job_ids: list[int] | None = None) -> bool:
        """Return True if all selected jobs are terminated in DB."""
        ids = job_ids if job_ids is not None else list(self.exe_data["jobs"])
        for job_id in ids:
            db_job: Job | None = session.get(Job, job_id)
            if not db_job:
                return False
            if db_job.status not in JobStatus.terminated_list():
                return False
        return True

    def run(self):
        """Process resurrection output and update job rows.

        Workflow
        --------
        1. Reload `ExeData` from disk to capture Executor/file-system updates.
        2. In recovery-only mode, scan log/output files to refresh results.
        3. Update each DB job row according to parsed result availability.

        Status policy
        -------------
        - valid result -> `DONE`
        - invalid numerical result -> `FAILED`
        - missing result in active mode -> `FAILED`
        - missing result in recovery-only mode -> restore original pre-recovery status
        """
        # > Re-load to capture changes made by Executor (if any) or filesystem
        self.exe_data.load()

        if self._recover_jobs:
            # > Recovery-only scan: update ExeData from logs found on disk.
            self.exe_data.scan_dir(force=True)
            self.exe_data.write(force=True)
        elif not self.exe_data.is_final:
            # > Active mode requires ExeData to be finalized by Executor
            raise RuntimeError(f"Job at {self.rel_path} did not finalize correctly.")

        with self.session as session:
            self._logger(
                session,
                f"{self._logger_prefix}::run:  {self.rel_path}",
            )

            jobs: dict[int, Job | None] = {
                job_id: session.get(Job, job_id) for job_id in self.exe_data["jobs"]
            }
            self._update_job(session, self.exe_data, jobs)
            for job_id, job in jobs.items():
                if job and job.status == JobStatus.RECOVER:
                    restored_status: JobStatus = self._recover_jobs.get(job.id, {}).get(
                        "status", JobStatus.RUNNING
                    )
                    if restored_status == JobStatus.RECOVER:
                        self._logger(
                            session,
                            f"{self._logger_prefix}::run:  job {job_id} "
                            + "has RECOVER as recovery status: overriding to RUNNING",
                            level=LogLevel.WARN,
                        )
                        restored_status = JobStatus.RUNNING
                    job.status = JobStatus.FAILED if not self._recover_jobs else restored_status

            self._safe_commit(session)

            # In recovery-only mode, finalize ExeData once tracked jobs are terminated.
            if self._recover_jobs and self._all_jobs_terminated(session, list(self._recover_jobs)):
                self.exe_data.finalize()
