# SPDX-FileCopyrightText: © 2024-present NNLOJET
#
# SPDX-License-Identifier: MIT

from ._exe import Executor
import subprocess
import json
import os
import re
from time import time


class LocalExec(Executor):
    """Execution on the local machine"""

    @property
    def resources(self):
        return {"ncores": self.ncores}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def exe(self):
        print("   >>>   LocalExec {}".format(self.local_path))

        job_env = os.environ.copy()
        job_env["OMP_NUM_THREADS"] = "{}".format(self.ncores)
        job_env["OMP_STACKSIZE"] = "1024M"

        start_time = time()
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

        self._result["elapsed_time"] = elapsed_time

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
                    self._result["iterations"].append(iteration)
                    iteration = {}

        # > save the accumulated result
        self._result["integral"] = self._result["iterations"][-1]["acc_val"]
        self._result["error"] = self._result["iterations"][-1]["acc_err"]
        self._result["chi2dof"] = self._result["iterations"][-1]["chi"]

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
            self._result["output_files"].append(file)

        with open(self._local(Executor._file_tmp), "w") as outfile:
            json.dump(self._result, outfile, indent=2)
