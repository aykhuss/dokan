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

    This task re-attaches to an existing job directory, spawns an `Executor`
    to ensure it completes (or collects existing results), and updates the
    database status.

    Attributes
    ----------
    rel_path : str
        Relative path to the job execution directory.

    """

    rel_path: str = luigi.Parameter()

    priority = 200

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # > pick up from where we left off
        self.exe_data: ExeData = ExeData(self._local(self.rel_path))

    def requires(self):
        """Require an Executor to run/check the job."""
        with self.session as session:
            self._debug(session, f"DBResurrect::requires:  rel_path = {self.rel_path}")
        return [
            Executor.factory(
                policy=self.exe_data["policy"],
                path=str(self.exe_data.path.absolute()),
            )
        ]

    def complete(self) -> bool:
        """Check if all jobs in this resurrection context are terminated."""
        with self.session as session:
            self._debug(session, f"DBResurrect::complete:  rel_path = {self.rel_path}")
            for job_id in self.exe_data["jobs"].keys():
                job = session.get(Job, job_id)
                if not job or job.status not in JobStatus.terminated_list():
                    self._debug(
                        session, f"DBResurrect::complete:  rel_path = {self.rel_path}: FALSE"
                    )
                    return False
            self._debug(session, f"DBResurrect::complete:  rel_path = {self.rel_path}: TRUE")
        return True

    def run(self):
        """Process the results of the resurrection execution."""
        # > need to re-load as state can be cached & not reflect the result
        self.exe_data.load()

        # > parse the Executor return data
        if not self.exe_data.is_final:
            raise RuntimeError(f"Job at {self.rel_path} did not finalize correctly.")

        with self.session as session:
            self._logger(
                session, f"DBResurrect::run:  rel_path = {self.rel_path}, run_tag = {self.run_tag}"
            )

            for job_id, job_entry in self.exe_data["jobs"].items():
                db_job: Job | None = session.get(Job, job_id)
                if not db_job:
                    self._logger(
                        session,
                        f"Job {job_id} not found in DB during resurrection",
                        level=LogLevel.DEBUG,
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
                    db_job.status = JobStatus.FAILED

            self._safe_commit(session)

        # @todo add automatic re-merge trigger like in `DBRunner`?
