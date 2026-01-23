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

    def requires(self):
        """Require an Executor to run/check the job."""
        if self.only_recover:
            return []
        with self.session as session:
            self._debug(session, f"DBResurrect::requires:  rel_path = {self.rel_path}")
        return [
            Executor.factory(
                policy=self.exe_data["policy"],
                path=str(self.exe_data.path.absolute()),
            )
        ]

    def complete(self) -> bool:
        """Check if all jobs in this resurrection context are terminated.

        Returns True if all associated jobs are in a terminated state (DONE, MERGED, FAILED)
        in the database.
        """
        with self.session as session:
            self._debug(session, f"DBResurrect::complete: {self.rel_path}")
            for job_id in self.exe_data["jobs"].keys():
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

    def run(self):
        """Process the results of the resurrection execution."""
        # > Re-load to capture changes made by Executor (if any) or filesystem
        self.exe_data.load()

        if self.only_recover:
            # Passive scan: update ExeData from logs found on disk
            self.exe_data.scan_dir()
        elif not self.exe_data.is_final:
            # Active mode requires ExeData to be finalized by Executor
            raise RuntimeError(f"Job at {self.rel_path} did not finalize correctly.")

        with self.session as session:
            self._logger(session, f"DBResurrect::run:  {self.rel_path}, run_tag = {self.run_tag}")

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
                    if math.isnan(res * err):
                        db_job.status = JobStatus.FAILED
                    else:
                        db_job.result = res
                        db_job.error = err
                        db_job.chi2dof = float(job_entry["chi2dof"])
                        db_job.elapsed_time = float(job_entry["elapsed_time"])
                        db_job.status = JobStatus.DONE
                else:
                    # Active mode: missing result implies failure
                    # Passive mode: missing result implies still incomplete
                    db_job.status = JobStatus.FAILED if not self.only_recover else JobStatus.RUNNING

            self._safe_commit(session)
