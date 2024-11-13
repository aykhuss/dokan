import luigi
import subprocess
import json
import os
import re
from time import time
from pathlib import Path

from ._executor import Executor


# > just a master class to collect common properties for local execution
class LocalExec(Executor):
    """Execution on the local machine"""

    # > only runs *one* seed: extra argument to pick the job,
    # > DBRunner responsible to dispatch a task for each seed!
    local_ncores: int = luigi.OptionalIntParameter(default=1)

    @property
    def resources(self):
        return {
            "local_ncores": self.local_ncores,
            "jobs_concurrent": self.local_ncores,
        }


class BatchLocalExec(LocalExec):
    """Batch execution on the local machine"""

    def requires(self):
        # print(f"BatchLocalExec: requires {[job_id for job_id in self.exe_data["jobs"].keys()]}")
        return [
            self.clone(cls=SingleLocalExec, job_id=job_id)
            for job_id in self.exe_data["jobs"].keys()
        ]

    def exe(self):
        # print(f"BatchLocalExec: exe {self.path}")
        pass


class SingleLocalExec(LocalExec):
    """Execution of a *single* job on the local machine"""

    job_id: int = luigi.IntParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # > use dict.get() -> int | None for non-throwing access
        self.seed: int = self.exe_data["jobs"][self.job_id]["seed"]
        # > extra output & error files
        self.file_out: Path = Path(self.path) / f"job.s{self.seed}.out"
        self.file_err: Path = Path(self.path) / f"job.s{self.seed}.err"

    def output(self):
        return [luigi.LocalTarget(self.file_out)]

    def exe(self):
        # print(f"SingleLocalExec: exe {self.path}")
        # > should never run since we overwrote run!
        raise RuntimeError("SingleLocalExec: exe should never be called")

    def run(self):
        # print(f"SingleLocalExec: run {self.path}")

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
                # raise RuntimeError("NNLOJET exited with an error")
                return  # safer way to exit -> Task will be flagged as "failed"
