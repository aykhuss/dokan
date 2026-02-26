"""Dokan Job Doctor.

Defines a task to check and repair/synchronize the database state of jobs with their on-disk data
"""

import re
import shutil
from pathlib import Path

import luigi
from sqlalchemy import select

from ..exe import ExeData
from ._dbtask import DBTask
from ._loglevel import LogLevel
from ._sqla import Job


class DBDoctor(DBTask):
    """Repair/synchronize DB job rows from on-disk execution directories.

    The task iterates over `rel_paths`, optionally refreshes `ExeData` from
    filesystem artifacts (`scan_dir=True`), then updates/creates corresponding DB
    `Job` rows via `_update_job`.
    """

    rel_paths: list[str] = luigi.ListParameter()
    scan_dir: bool = luigi.BoolParameter(default=False)

    priority = 200

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger_prefix: str = self.__class__.__name__
        if self.scan_dir:
            self._logger_prefix += "[dim](scan-dir)[/dim]"

    def requires(self):
        """Doctoring has no Luigi task dependencies."""
        return []

    def complete(self) -> bool:
        """Return True once all known execution metadata is at/after `run_tag`."""
        for rp in self.rel_paths:
            exe_dir: Path = self._local(rp)
            if not exe_dir.exists() or not exe_dir.is_dir():
                continue
            exe_data = ExeData(exe_dir)
            if exe_data.st_mtime < self.run_tag:
                return False
        return True

    def run(self) -> None:
        """Process each path and synchronize DB rows with `ExeData` content."""

        for rp in self.rel_paths:
            exe_dir: Path = self._local(rp)
            if not exe_dir.exists() or not exe_dir.is_dir():
                continue
            exe_data = ExeData(exe_dir)
            if exe_data.st_mtime < self.run_tag:
                if self.scan_dir:
                    exe_data.scan_dir(force=True, reset_output=True)
                    exe_data.write(force=True)
                else:
                    _ = exe_data.touch
            # > @todo: if --check-grids: try grid evaluation
            # > @todo: other sanity checks that are potentially costly and don't require DB access

            with self.session as session:
                # > register all DB jobs for this exe path
                jobs: dict[int, Job | None] = {
                    j.id: j for j in session.scalars(select(Job).where(Job.rel_path == rp))
                }
                # > add any additional jobs registered in exe_data but missing from DB
                for job_id in exe_data.get("jobs", {}):
                    if job_id not in jobs:
                        jobs[int(job_id)] = None  # marked for add/update below

                self._logger(
                    session, f"{self._logger_prefix}::run:  processing {rp} [dim]({len(jobs)} jobs)[/dim]"
                )

                if jobs:
                    self._update_job(session, exe_data, jobs, add_missing=True, skip_terminated=False)
                else:
                    # > find files that match job execution results in the same folder
                    other_files = [p for p in exe_dir.iterdir() if re.match(r".*\.s\d+\..*", p.name)]
                    # > only delete directory tree if there really are no potential "left overs"
                    if not other_files:
                        try:
                            shutil.rmtree(exe_dir)
                        except Exception as exc:
                            self._logger(
                                session,
                                f"{self._logger_prefix}::run: failed to remove "
                                + f"empty job folder at {exe_dir}: {exc!r}",
                                level=LogLevel.WARN,
                            )
                    else:
                        self._logger(
                            session,
                            f"{self._logger_prefix}::run: not removing dir {exe_dir} "
                            + f"even though ExeData is empty since other files ({other_files}) are still present",
                            level=LogLevel.WARN,
                        )
