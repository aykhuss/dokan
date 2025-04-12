"""NNLOJET execution interface

defines an abstraction to execute NNLOJET on different backends (policies)
a factory design pattern to obtain tasks for the different policies
"""

import datetime
import os
import time
from abc import ABCMeta, abstractmethod
from pathlib import Path

import luigi

from .._types import GenericPath
from ..db._loglevel import LogLevel
from ..nnlojet import parse_log_file
from ._exe_config import ExecutionPolicy
from ._exe_data import ExeData


class Executor(luigi.Task, metaclass=ABCMeta):
    _file_log: str = "exe.log"

    path: str = luigi.Parameter()
    log_level: LogLevel = luigi.OptionalIntParameter(default=LogLevel.INFO)

    priority = 100

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exe_data: ExeData = ExeData(Path(self.path))
        # > we just use a log file to collect output
        self.file_log: Path = Path(self.path) / self._file_log

        # self.logger = logging.getLogger(f"{self.__class__.__name__}:{self.__hash__()}")
        # self.logger_fh = logging.FileHandler(
        #     Path(self.path) / self._file_log, mode="a", encoding="utf-8"
        # )
        # formatter = logging.Formatter(
        #     "{asctime}({levelname}): {message}",
        #     style="{",
        #     datefmt="%Y-%m-%d %H:%M",
        # )
        # self.logger_fh.setFormatter(formatter)
        # self.logger_fh.setLevel(self.log_level)
        # self.logger.addHandler(self.logger_fh)
        # self.logger.error(f"{self.__class__.__name__}:{self.__hash__()}")
        # self.logger.debug("init debug")
        # self.logger.info("init info")
        # self.logger.warn("init warn")
        # self.logger.error("init error")
        # print(self.logger.handlers)

    def _logger(self, message: str, level: LogLevel = LogLevel.INFO) -> None:
        # > pass through log level & all signales
        if level >= 0 and level < self.log_level:
            return
        # > write to log file
        dt_str: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.file_log, "at") as lf:
            lf.write(f"[{dt_str}]({level!r}): {message}\n")

    def _debug(self, message: str) -> None:
        self._logger(message, LogLevel.DEBUG)

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
        raise NotImplementedError("Executor::exe: abstract method must be overridden!")

    def run(self):
        # > more preparation for execution?

        # @todo check if job files are already there? (recovery mode?)

        # > some systems have a different resolution in timestamps
        # > time.time() vs. os.stat().st_mtime
        # > this buffer ensures time ordering works
        time.sleep(1.5)

        # > call the backend specific execution
        try:
            self.exe()
        except Exception as e:
            self._logger(f"exception in exe: {e}", level=LogLevel.ERROR)
            # (continue workflow; finalize ExeData)  raise

        # > collect files that were generated/modified in this execution
        # > some file systems have delays: add delays & re-tries to be safe
        fs_max_retry: int = 10
        fs_delay: float = 1.0  # seconds
        for _ in range(fs_max_retry):
            for entry in os.scandir(self.path):
                if entry.name in [ExeData._file_tmp, ExeData._file_fin, self._file_log]:
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
                self._logger(
                    f"Executor: log file not found for job {job_id} in {self.path}: {log_matches}",
                    level=LogLevel.WARN,
                )
                continue
            parsed_data = parse_log_file(Path(self.path) / log_matches[0])
            for key in parsed_data:
                job_data[key] = parsed_data[key]

        # > mark execution complete
        self.exe_data.finalize()
