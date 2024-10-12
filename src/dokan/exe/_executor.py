"""NNLOJET execution interface

defines an abstraction to execute NNLOJET on different backends (policies)
a factory design pattern to obtain tasks for the different policies
"""

import luigi


from abc import ABCMeta, abstractmethod
from pathlib import Path
from os import PathLike
import time

from ._exe_config import ExecutionMode, ExecutionPolicy
from ._exe_data import ExeData
import dokan.runcard


class Executor(luigi.Task, metaclass=ABCMeta):
    path: str = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exe_data: ExeData = ExeData(Path(self.path))

    @staticmethod
    def factory(policy=ExecutionPolicy.LOCAL, *args, **kwargs):
        # > local import to avoid cyclic dependence
        from ._local import LocalExec

        if policy == ExecutionPolicy.LOCAL:
            return LocalExec(*args, **kwargs)

        # if policy == ExecutionPolicy.HTCONDOR:
        #     return HTCondorExec(*args, **kwargs)

        raise TypeError(f"invalid ExecutionPolicy: {policy!r}")

    def requires(self):
        return []

    def output(self):
        return [luigi.LocalTarget(self.exe_data.file_fin)]

    @abstractmethod
    def exe(self) -> None:
        pass

    def run(self):
        self.exe_data["timestamp"] = time.time()
        # > more preparation for execution?
        # @todo check if job files are already there? (recovery mode?)
        self.exe()
        # > exe done, generate the output file.
        self.exe_data["timestamp"] = time.time()
        # @todo: loop over all generated files, parse log files to populate results and errors
        self.exe_data.finalize()
