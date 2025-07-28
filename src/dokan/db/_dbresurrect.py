"""dokan job resurrection

define a task attempting to resurrect a job that is in a `RUNNING` state
from an old run. A previous run might have been cancelled or failed due
to the loss of a ssh connection.
"""

import math

import luigi

from ..exe import Executor, ExeData
from ._dbtask import DBTask
from ._jobstatus import JobStatus
from ._sqla import Job


class DBResurrect(DBTask):
    rel_path: str = luigi.Parameter()

    priority = 200

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # > pick up from where we left off
        self.exe_data: ExeData = ExeData(self._local(self.rel_path))

    def requires(self):
        with self.session as session:
            self._debug(session, f"DBResurrect::requires:  rel_path = {self.rel_path}")
        return [
            Executor.factory(
                policy=self.exe_data["policy"],
                path=str(self.exe_data.path.absolute()),
            )
        ]

    def complete(self) -> bool:
        with self.session as session:
            self._debug(session, f"DBResurrect::complete:  rel_path = {self.rel_path}")
            for job_id in self.exe_data["jobs"].keys():
                if session.get_one(Job, job_id).status not in JobStatus.terminated_list():
                    self._debug(session, f"DBResurrect::complete:  rel_path = {self.rel_path}: FALSE")
                    return False
            self._debug(session, f"DBResurrect::complete:  rel_path = {self.rel_path}: TRUE")
        return True

    def run(self):
        # > need to re-load as state can be cached & not reflect the result
        self.exe_data = ExeData(self._local(self.rel_path))

        # > parse the Executor retun data
        if not self.exe_data.is_final:
            raise RuntimeError(f"{self.rel_path} not final?!\n{self.exe_data.data}")

        with self.session as session:
            self._logger(session, f"DBResurrect::run:  rel_path = {self.rel_path}, run_tag = {self.run_tag}")
            for job_id, job_entry in self.exe_data["jobs"].items():
                db_job: Job = session.get_one(Job, job_id)
                if "result" in job_entry:
                    if math.isnan(float(job_entry["result"]) * float(job_entry["error"])):
                        db_job.status = JobStatus.FAILED
                    else:
                        db_job.result = job_entry["result"]
                        db_job.error = job_entry["error"]
                        db_job.chi2dof = job_entry["chi2dof"]
                        db_job.elapsed_time = job_entry["elapsed_time"]
                        db_job.status = JobStatus.DONE
                else:
                    db_job.status = JobStatus.FAILED
            self._safe_commit(session)

        # @todo add automatic re-merge trigger like in `DBRunner`?
