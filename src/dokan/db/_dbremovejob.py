"""Dokan Job Removal.

Defines a task to cleanly remove a job both from the database and the file system.
"""

import re
import shutil

import luigi
from sqlalchemy import select

from dokan.db._loglevel import LogLevel

from ..exe import ExeData
from ._dbtask import DBTask
from ._sqla import Job


class DBRemoveJob(DBTask):
    """Remove one job from the DB and (optionally) from its execution metadata.

    The task is idempotent: if the job no longer exists in the database,
    `complete()` returns True and `run()` becomes a no-op.

    If removing the job empties its `ExeData["jobs"]` map, the task may also
    remove the corresponding execution directory when no other DB jobs and no
    seed-tagged output files are detected there.
    """

    job_id: int = luigi.IntParameter()

    priority = 200

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger_prefix: str = self.__class__.__name__ + f"[dim](id={self.job_id})[/dim]"

    def complete(self) -> bool:
        """Return True when the target job row has been removed."""
        with self.session as session:
            job: Job | None = session.get(Job, self.job_id)
            return bool(not job)

    def run(self) -> None:
        """Delete the job row and clean associated on-disk results.

        Cleanup is attempted first so `ExeData` can still resolve seed/path
        metadata from the DB row. DB deletion is always attempted afterwards,
        even if filesystem cleanup encounters errors.
        """
        with self.session as session:
            self._logger(session, f"{self._logger_prefix}::run")
            rm_job: Job | None = session.get(Job, self.job_id)
            if not rm_job:
                return

            if rm_job.rel_path:
                job_path = self._local(rm_job.rel_path)
                if job_path.exists():
                    exe_data = ExeData(job_path)
                    cleanup_ok = True
                    try:
                        exe_data.remove_job(self.job_id, force=True)
                    except Exception as exc:
                        cleanup_ok = False
                        self._logger(
                            session,
                            f"{self._logger_prefix}::run: failed ExeData cleanup at {job_path}: {exc!r}",
                            level=LogLevel.WARN,
                        )

                    # > if the folder has no jobs left, we can remove it entirely
                    if cleanup_ok and not exe_data["jobs"]:
                        # > find all jobs in the DB that have the same `rel_path`
                        other_jobs = [
                            oj.id
                            for oj in session.scalars(select(Job).where(Job.rel_path == rm_job.rel_path))
                        ]
                        # > find files that match job execution results in the same folder
                        other_files = [p for p in job_path.iterdir() if re.match(r".*\.s\d+\..*", p.name)]
                        # > only delete directory tree if there really is no potential "left overs"
                        if set(other_jobs) == {self.job_id} and not other_files:
                            try:
                                shutil.rmtree(job_path)
                            except Exception as exc:
                                self._logger(
                                    session,
                                    f"{self._logger_prefix}::run: failed to remove "
                                    + f"empty job folder at {job_path}: {exc!r}",
                                    level=LogLevel.WARN,
                                )
                        else:
                            self._logger(
                                session,
                                f"{self._logger_prefix}::run: not removing dir {job_path} "
                                + f"even though ExeData is empty since other jobs ({other_jobs}) "
                                + f"or files ({other_files}) are still present",
                                level=LogLevel.WARN,
                            )

            session.delete(rm_job)
            self._safe_commit(session)
