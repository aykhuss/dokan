"""NNLOJET execution on the local machine

implementation of the backend for ExecutionPolicy.LOCAL
"""

import os
import subprocess
from pathlib import Path

import luigi

from ...db._loglevel import LogLevel

from .._executor import Executor


class LocalExec(Executor):
    """Abstract base class for local execution

    Attributes
    ----------
    local_ncores : int
        number of cores to use on the local machine
    """

    local_ncores: int = luigi.OptionalIntParameter(default=1)


class BatchLocalExec(LocalExec):
    """Wrapper task to batch-execute multiple local jobs"""

    def requires(self):
        return [
            self.clone(cls=SingleLocalExec, job_id=job_id)
            for job_id in self.exe_data["jobs"].keys()
        ]

    def exe(self):
        pass


class SingleLocalExec(LocalExec):
    """Task to execute a *single* job on the local machine

    Attributes
    ----------
    job_id : int
        id of the job defined in exe_data to execute
    """

    job_id: int = luigi.IntParameter()

    @property
    def resources(self):
        return {
            "local_ncores": self.local_ncores,
            # "jobs_concurrent": self.local_ncores,
            "jobs_concurrent": 1,
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # > use dict.get() -> int | None for non-throwing access?
        self.seed: int = self.exe_data["jobs"][self.job_id]["seed"]
        # > extra output & error files
        self.file_out: Path = Path(self.path) / f"job.s{self.seed}.out"
        self.file_err: Path = Path(self.path) / f"job.s{self.seed}.err"

    def output(self):
        return [luigi.LocalTarget(self.file_out)]

    def exe(self):
        # > should never run since `run` is overridden
        raise RuntimeError("SingleLocalExec::exe: should never be called")

    def run(self):
        job_env = os.environ.copy()
        job_env["OMP_NUM_THREADS"] = "{}".format(self.local_ncores)
        job_env["OMP_STACKSIZE"] = "1024M"

        with open(self.file_out, "w") as of, open(self.file_err, "w") as ef:
            exec_out = subprocess.run(
                [
                    self.exe_data["exe"],
                    "-run",
                    self.exe_data["input_files"][0],
                    "-iseed",
                    str(self.seed),
                ],
                env=job_env,
                cwd=self.path,
                stdout=of,
                stderr=ef,
                text=True,
            )
            if exec_out.returncode != 0:
                self._logger(
                    f"SingleLocalExec failed to execute job {self.path}", level=LogLevel.ERROR
                )
                return  # job will be flagged "failed"
