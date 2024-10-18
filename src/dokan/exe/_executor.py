"""NNLOJET execution interface

defines an abstraction to execute NNLOJET on different backends (policies)
a factory design pattern to obtain tasks for the different policies
"""

import luigi
import logging

from abc import ABCMeta, abstractmethod
from pathlib import Path
import os
import time

from ._exe_config import ExecutionMode, ExecutionPolicy
from ._exe_data import ExeData
from ..nnlojet import parse_log_file

logger = logging.getLogger("luigi-interface")


class Executor(luigi.Task, metaclass=ABCMeta):
    path: str = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exe_data: ExeData = ExeData(Path(self.path))

    @staticmethod
    def factory(policy=ExecutionPolicy.LOCAL, *args, **kwargs):
        # > local import to avoid cyclic dependence
        from ._local import BatchLocalExec

        if policy == ExecutionPolicy.LOCAL:
            print(f"factory: BatchLocalExec ...")
            return BatchLocalExec(*args, **kwargs)

        # if policy == ExecutionPolicy.HTCONDOR:
        #     return HTCondorExec(*args, **kwargs)

        raise TypeError(f"invalid ExecutionPolicy: {policy!r}")

    def output(self):
        return [luigi.LocalTarget(self.exe_data.file_fin)]

    @abstractmethod
    def exe(self):
        pass

    def run(self):
        print(f"[{time.time()}] Executor: run {self.path}")

        # > more preparation for execution?

        # @todo check if job files are already there? (recovery mode?)

        self.exe()
        print(f"[{time.time()}] Executor: done with exe {self.path}")

        # > exe done populate job data and write target file

        # > keep track of files that were generated
        for entry in os.scandir(self.path):
            if entry.name in self.exe_data["input_files"]:
                continue
            if entry.name in [ExeData._file_tmp, ExeData._file_fin]:
                continue
            print(f"> [{entry.stat().st_mtime} < {self.exe_data["timestamp"]} {entry.stat().st_mtime < self.exe_data["timestamp"]}] {self.path}: {entry.name}")
            if entry.stat().st_mtime < self.exe_data["timestamp"]:
                continue
            # > genuine output file that was generated/modified
            self.exe_data["output_files"].append(entry.name)

        print(f"  >>  {self.exe_data["output_files"]}:")

        for job_id, job_data in self.exe_data["jobs"].items():
            print(f".s{job_data["seed"]}.log")
            log_matches = [
                of
                for of in self.exe_data["output_files"]
                if of.endswith(f".s{job_data["seed"]}.log")
            ]
            print(f" > log_matches: {log_matches}")
            if len(log_matches) != 1:
                continue
            parsed_data = parse_log_file(Path(self.path) / log_matches[0])
            print(f" > parsed data: {parsed_data}")
            for key in parsed_data:
                job_data[key] = parsed_data[key]

        print(f"[{time.time()}] Executor: finalize {self.path}")
        # > add last modification time?
        self.exe_data.finalize()
