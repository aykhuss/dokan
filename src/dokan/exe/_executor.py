"""NNLOJET execution interface

defines an abstraction to execute NNLOJET on different backends (policies)
a factory design pattern to obtain tasks for the different policies
"""

import logging
import os
import time
from abc import ABCMeta, abstractmethod
from pathlib import Path

import luigi

from ..nnlojet import parse_log_file
from ._exe_config import ExecutionPolicy
from ._exe_data import ExeData
from .._types import GenericPath

logger = logging.getLogger("luigi-interface")


class Executor(luigi.Task, metaclass=ABCMeta):
    path: str = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exe_data: ExeData = ExeData(Path(self.path))

    @staticmethod
    def get_cls(policy: ExecutionPolicy):
        # > local import to avoid cyclic dependence
        from .htcondor import HTCondorExec
        from .local import BatchLocalExec
        from .slurm import SlurmExec

        if policy == ExecutionPolicy.LOCAL:
            return BatchLocalExec

        if policy == ExecutionPolicy.HTCONDOR:
            return HTCondorExec

        if policy == ExecutionPolicy.SLURM:
            return SlurmExec

        raise TypeError(f"invalid ExecutionPolicy: {policy!r}")

    @staticmethod
    def factory(policy: ExecutionPolicy = ExecutionPolicy.LOCAL, *args, **kwargs):
        """factory method to create an Executor for a specific policy"""

        exec_cls = Executor.get_cls(policy)
        return exec_cls(*args, **kwargs)

    @staticmethod
    def templates() -> list[GenericPath]:
        """list of built-in templates for this executor

        if the executor requires additional template files, such as submission
        files, these should be provided through this method though an override

        Returns
        -------
        list[GenericPath]
            a list of all built-in template files for the executor
        """
        return []

    def output(self):
        return [luigi.LocalTarget(self.exe_data.file_fin)]

    @abstractmethod
    def exe(self):
        raise NotImplementedError("Executor::exe")

    def run(self):
        # > more preparation for execution?

        # @todo check if job files are already there? (recovery mode?)

        # > some systems have a different resolution in timestamps
        # > time.time() vs. os.stat().st_mtime
        # > this buffer ensures time ordering works
        time.sleep(1.5)

        # > call the backend specific execution
        self.exe()

        # > collect files that were generated/modified in this execution
        # > some file systems have delays: add delays & re-tries
        fs_max_retry: int = 10
        fs_delay: float = 1.0  # seconds
        for _ in range(fs_max_retry):
            for entry in os.scandir(self.path):
                if entry.name in [ExeData._file_tmp, ExeData._file_fin]:
                    continue
                if entry.stat().st_mtime < self.exe_data.timestamp:
                    continue
                # > genuine output file that was generated/modified
                self.exe_data["output_files"].append(entry.name)
            if len(self.exe_data["output_files"]) > 0:
                break
            # > could not find any output files, wait and try again
            time.sleep(fs_delay)

        # > save information from logs in exe_data
        for job_id, job_data in self.exe_data["jobs"].items():
            log_matches = [
                of
                for of in self.exe_data["output_files"]
                if of.endswith(f".s{job_data['seed']}.log")
            ]
            if len(log_matches) != 1:
                logging.warn(f"Executor: log file not found for job {job_id}")
                continue
            parsed_data = parse_log_file(Path(self.path) / log_matches[0])
            for key in parsed_data:
                job_data[key] = parsed_data[key]

        # > mark execution complete
        self.exe_data.finalize()
