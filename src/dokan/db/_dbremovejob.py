"""Dokan Job Removal.

Defines a task to cleanly remove a job both from the database and the file system.
"""

from pathlib import Path

import luigi

from dokan.db._loglevel import LogLevel

from ..exe import ExeData
from ._dbtask import DBTask
from ._sqla import Job


class DBRemoveJob(DBTask):
    """Remove one job from the DB and (optionally) from its execution metadata.

    The task is idempotent: if the job no longer exists in the database,
    `complete()` returns True and `run()` becomes a no-op.
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
        metadata from the DB row. DB deletion is always attempted afterwards.
        """
        with self.session as session:
            self._logger(session, f"{self._logger_prefix}::run")
            job: Job | None = session.get(Job, self.job_id)
            if not job:
                return

            if job.rel_path:
                job_path: Path = self._local(job.rel_path)
                if job_path.exists():
                    try:
                        exe_data = ExeData(job_path)
                        exe_data.remove_job(self.job_id, force=True)
                    except Exception as exc:
                        self._logger(
                            session,
                            f"{self._logger_prefix}::run: failed ExeData cleanup at {job_path}: {exc!r}",
                            level=LogLevel.WARN,
                        )

            session.delete(job)
            self._safe_commit(session)
