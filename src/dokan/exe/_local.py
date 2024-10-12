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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_id__seed: dict[int, int] = {
            job_dict["job_id"]: job_dict["seed"] for job_dict in self.exe_data["jobs"]
        }

    @property
    def resources(self):
        return {"local_ncores": self.local_ncores}

    def exe(self):
        print("   >>>   LocalExec {}".format(self.local_path))

        job_env = os.environ.copy()
        job_env["OMP_NUM_THREADS"] = "{}".format(self.local_ncores)
        job_env["OMP_STACKSIZE"] = "1024M"

        start_time = time()  # save in exe after copying input files
        with open(self._local(Executor._file_out), "w") as of, open(
            self._local(Executor._file_err), "w"
        ) as ef:
            exec_out = subprocess.run(
                [self.config["exe"]["path"], "-run", Executor._file_run],
                env=job_env,
                cwd=self._path,
                stdout=of,
                stderr=ef,
                text=True,
            )
            if exec_out.returncode != 0:
                # raise RuntimeError("NNLOJET exited with an error")
                return  # safer way to exit -> Task will be flagged as "failed"
        elapsed_time = time() - start_time

        self.data["elapsed_time"] = elapsed_time

        # > parse the output file to extract some information
        with open(self._local(Executor._file_out), "r") as of:
            iteration = {}
            for line in of:
                match_iteration = re.search(
                    r"\(\s*iteration\s+(\d+)\s*\)", line, re.IGNORECASE
                )
                if match_iteration:
                    iteration["nit"] = int(match_iteration.group(1))
                match_integral = re.search(
                    r"\bintegral\s*=\s*(\S+)\s+accum\.\s+integral\s*=\s*(\S+)\b",
                    line,
                    re.IGNORECASE,
                )
                if match_integral:
                    iteration["val"] = float(match_integral.group(1))
                    iteration["acc_val"] = float(match_integral.group(2))
                match_stddev = re.search(
                    r"\bstd\.\s+dev\.\s*=\s*(\S+)\s+accum\.\s+std\.\s+dev\s*=\s*(\S+)\b",
                    line,
                    re.IGNORECASE,
                )
                if match_stddev:
                    iteration["err"] = float(match_stddev.group(1))
                    iteration["acc_err"] = float(match_stddev.group(2))
                match_chi2it = re.search(
                    r"\schi\*\*2/iteration\s*=\s*(\S+)\b", line, re.IGNORECASE
                )
                if match_chi2it:
                    iteration["chi"] = float(match_chi2it.group(1))
                    self.data["iterations"].append(iteration)
                    iteration = {}

        # > save the accumulated result
        self.data["integral"] = self.data["iterations"][-1]["acc_val"]
        self.data["error"] = self.data["iterations"][-1]["acc_err"]
        self.data["chi2dof"] = self.data["iterations"][-1]["chi"]

        # > keep track of files that were generated
        # > new recommendation is to use os.scandir()
        for file in os.listdir(self._path):
            if file in [
                Executor._file_run,
                Executor._file_res,
                Executor._file_tmp,
                Executor._file_out,
                Executor._file_err,
            ]:
                continue
            if os.stat(self._local(file)).st_mtime < start_time:
                continue
            # > genuine output file that was generated/modified
            self.data["output_files"].append(file)

        with open(self._local(Executor._file_tmp), "w") as outfile:
            json.dump(self.data, outfile, indent=2)


class SingleLocalExec(LocalExec):
    """Execution of a *single* job on the local machine"""

    job_id: int = luigi.IntParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        #> use dict.get() -> int | None for non-throwing access
        self.seed: int = self.job_id__seed[self.job_id]
        # > extra output & error files
        self.file_out: Path = Path(self.path) / f"job.s{self.seed}.out"
        self.file_err: Path = Path(self.path) / f"job.s{self.seed}.err"

    def output(self):
        return []

    def complete(self):
        pass #@todo: implement

    def run(self):
        print("   >>>   SingleLocalExec {}".format(self.path))

        job_env = os.environ.copy()
        job_env["OMP_NUM_THREADS"] = "{}".format(self.local_ncores)
        job_env["OMP_STACKSIZE"] = "1024M"



        with open(file_out, "w") as of, open(file_err, "w") as ef:
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
        elapsed_time = time() - self.exe_data["timestamp"]

        self.exe_data["elapsed_time"] = elapsed_time

        # > parse the output file to extract some information
        with open(self._local(Executor._file_out), "r") as of:
            iteration = {}
            for line in of:
                match_iteration = re.search(
                    r"\(\s*iteration\s+(\d+)\s*\)", line, re.IGNORECASE
                )
                if match_iteration:
                    iteration["nit"] = int(match_iteration.group(1))
                match_integral = re.search(
                    r"\bintegral\s*=\s*(\S+)\s+accum\.\s+integral\s*=\s*(\S+)\b",
                    line,
                    re.IGNORECASE,
                )
                if match_integral:
                    iteration["val"] = float(match_integral.group(1))
                    iteration["acc_val"] = float(match_integral.group(2))
                match_stddev = re.search(
                    r"\bstd\.\s+dev\.\s*=\s*(\S+)\s+accum\.\s+std\.\s+dev\s*=\s*(\S+)\b",
                    line,
                    re.IGNORECASE,
                )
                if match_stddev:
                    iteration["err"] = float(match_stddev.group(1))
                    iteration["acc_err"] = float(match_stddev.group(2))
                match_chi2it = re.search(
                    r"\schi\*\*2/iteration\s*=\s*(\S+)\b", line, re.IGNORECASE
                )
                if match_chi2it:
                    iteration["chi"] = float(match_chi2it.group(1))
                    self.data["iterations"].append(iteration)
                    iteration = {}

        # > save the accumulated result
        self.data["integral"] = self.data["iterations"][-1]["acc_val"]
        self.data["error"] = self.data["iterations"][-1]["acc_err"]
        self.data["chi2dof"] = self.data["iterations"][-1]["chi"]

        # > keep track of files that were generated
        # > new recommendation is to use os.scandir()
        for file in os.listdir(self._path):
            if file in [
                Executor._file_run,
                Executor._file_res,
                Executor._file_tmp,
                Executor._file_out,
                Executor._file_err,
            ]:
                continue
            if os.stat(self._local(file)).st_mtime < start_time:
                continue
            # > genuine output file that was generated/modified
            self.data["output_files"].append(file)

        with open(self._local(Executor._file_tmp), "w") as outfile:
            json.dump(self.data, outfile, indent=2)
