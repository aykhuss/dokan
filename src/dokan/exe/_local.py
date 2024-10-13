import luigi
import subprocess
import json
import os
import re
from time import time
from pathlib import Path

from ._executor import Executor


class LocalExec(Executor):
    """Execution on the local machine"""

    # > only runs *one* seed: extra argument to pick the job,
    # > DBRunner responsible to dispatch a task for each seed!
    local_ncores: int = luigi.OptionalIntParameter(default=1)

    @property
    def resources(self):
        return {"local_ncores": self.local_ncores}

    def exe(self):
        print("   >>>   LocalExec {}".format(self.path))

        yield [
            self.clone(cls=SingleLocalExec, job_id=job_id)
            for job_id in self.exe_data["jobs"].keys()
        ]


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

    def run(self):
        print("   >>>   SingleLocalExec {}".format(self.path))

        job_env = os.environ.copy()
        job_env["OMP_NUM_THREADS"] = "{}".format(self.local_ncores)
        job_env["OMP_STACKSIZE"] = "1024M"

        with open(self.file_out, "w") as of, open(self.file_err, "w") as ef:
            exec_out = subprocess.run(
                [self.exe_data["exe"], "-run", self.exe_data["input_files"][0]],
                env=job_env,
                cwd=self.path,
                stdout=of,
                stderr=ef,
                text=True,
            )
            if exec_out.returncode != 0:
                # raise RuntimeError("NNLOJET exited with an error")
                return  # safer way to exit -> Task will be flagged as "failed"

        # > alternative way of computing runtime rather than parsong the log
        # elapsed_time = time() - self.exe_data["timestamp"]
