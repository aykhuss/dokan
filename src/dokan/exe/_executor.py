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
        self.exe_data: ExeData = ExeData(Path(self.path), expect_tmp=True)

    @staticmethod
    def factory(policy=ExecutionPolicy.LOCAL, *args, **kwargs):
        # > local import to avoid cyclic dependence
        from ._local import LocalExec

        if policy == ExecutionPolicy.LOCAL:
            print(f"factory: LocalExec ...")
            return LocalExec(*args, **kwargs)

        # if policy == ExecutionPolicy.HTCONDOR:
        #     return HTCondorExec(*args, **kwargs)

        raise TypeError(f"invalid ExecutionPolicy: {policy!r}")

    def requires(self):
        return []

    def output(self):
        return [luigi.LocalTarget(self.exe_data.file_fin)]

    @abstractmethod
    def exe(self):
        pass

    def run(self):
        print(f"Executor: run {self.path}")
        self.exe_data["timestamp"] = time.time()
        # > more preparation for execution?
        # @todo check if job files are already there? (recovery mode?)
        self.exe()
        # > exe done, generate the output file.
        self.exe_data["timestamp"] = time.time()
        # @todo: loop over all generated files, parse log files to populate results and errors
        print(f"Executor: finalize {self.path}")
        self.exe_data.finalize()



        # # > parse the output file to extract some information
        # with open(self._local(Executor._file_out), "r") as of:
        #     iteration = {}
        #     for line in of:
        #         match_iteration = re.search(
        #             r"\(\s*iteration\s+(\d+)\s*\)", line, re.IGNORECASE
        #         )
        #         if match_iteration:
        #             iteration["nit"] = int(match_iteration.group(1))
        #         match_integral = re.search(
        #             r"\bintegral\s*=\s*(\S+)\s+accum\.\s+integral\s*=\s*(\S+)\b",
        #             line,
        #             re.IGNORECASE,
        #         )
        #         if match_integral:
        #             iteration["val"] = float(match_integral.group(1))
        #             iteration["acc_val"] = float(match_integral.group(2))
        #         match_stddev = re.search(
        #             r"\bstd\.\s+dev\.\s*=\s*(\S+)\s+accum\.\s+std\.\s+dev\s*=\s*(\S+)\b",
        #             line,
        #             re.IGNORECASE,
        #         )
        #         if match_stddev:
        #             iteration["err"] = float(match_stddev.group(1))
        #             iteration["acc_err"] = float(match_stddev.group(2))
        #         match_chi2it = re.search(
        #             r"\schi\*\*2/iteration\s*=\s*(\S+)\b", line, re.IGNORECASE
        #         )
        #         if match_chi2it:
        #             iteration["chi"] = float(match_chi2it.group(1))
        #             self.data["iterations"].append(iteration)
        #             iteration = {}

        # # > save the accumulated result
        # self.data["integral"] = self.data["iterations"][-1]["acc_val"]
        # self.data["error"] = self.data["iterations"][-1]["acc_err"]
        # self.data["chi2dof"] = self.data["iterations"][-1]["chi"]

        # # > keep track of files that were generated
        # # > new recommendation is to use os.scandir()
        # for file in os.listdir(self._path):
        #     if file in [
        #         Executor._file_run,
        #         Executor._file_res,
        #         Executor._file_tmp,
        #         Executor._file_out,
        #         Executor._file_err,
        #     ]:
        #         continue
        #     if os.stat(self._local(file)).st_mtime < start_time:
        #         continue
        #     # > genuine output file that was generated/modified
        #     self.data["output_files"].append(file)

        # with open(self._local(Executor._file_tmp), "w") as outfile:
        #     json.dump(self.data, outfile, indent=2)
