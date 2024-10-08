"""NNLOJET execution

module that defines all the different ways of executing NNLOJET
(platforms, modes, ...)
"""

from enum import IntEnum, unique
from abc import ABCMeta, abstractmethod
import luigi
import shutil
from pathlib import Path
from os import PathLike
import json
from time import time
from ..task import Task
# import hashlib

import dokan.runcard


@unique
class ExecutionPolicy(IntEnum):
    """policies on how/where to execute NNLOJET"""

    # NULL = 0
    LOCAL = 1
    HTCONDOR = 2
    # SLURM = 3
    # LSF = 4
    # ...

    def __str__(self):
        return self.name.lower()

    def __repr__(self):
        return str(self)

    @staticmethod
    def argparse(s: str):
        """method for `argparse`"""
        try:
            return ExecutionPolicy[s.upper()]
        except KeyError:
            return s


@unique
class ExecutionMode(IntEnum):
    """The two execution modes of NNLOJET"""

    # NULL = 0
    WARMUP = 1
    PRODUCTION = 2

    def __str__(self):
        return self.name.lower()


# > give all needed parameters for an NNLO calculation explicitly as parameters?
class Executor(Task, metaclass=ABCMeta):
    # > define some class-local variables for file name conventions
    _file_run: str = "job.run"
    _file_res: str = "job.json"
    _file_tmp: str = "job.tmp"

    policy: int = luigi.IntParameter()
    job_ids: list[int] = luigi.ListParameter()
    seeds: list[int] = luigi.ListParameter()
    mode: int = luigi.IntParameter()
    ncall: int = luigi.IntParameter()
    niter: int = luigi.IntParameter()
    # > if there's a previous warmup job pass the dir to it:
    # > need to get the `output_files` from there and copy over
    input_local_path: list = luigi.ListParameter(default=[])  # warmup we need

    # @todo: add options for overrides here?

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # > some consistency checks
        if len(self.job_ids) != len(self.seeds):
            raise ValueError("Executor: job_ids and seeds must have the same length")
        if (len(self.seeds) > 1) and all(
            self.seeds[i] + 1 == self.seeds[i + 1] for i in range(len(self.seeds) - 1)
        ):
            raise ValueError(
                f"Executor: seeds must be a consecutive list! {self.seeds}"
            )
        # > create data structure for the executor
        # @todo abstract away this datastructure to something more rigid like CONFIG?
        self.data = {
            "policy": self.policy,
            "policy_settings": {},
            # "job_ids": self.job_ids,
            # "seeds": self.seeds,
            "local_path": self.local_path,
            "mode": self.mode,
            "ncall": self.ncall,
            "niter": self.niter,
            "input_files": [],
            "output_files": [],
            "timestamp": None,
            "results": [
                {
                    "job_id": job_id,
                    "seed": seed,
                    "result": None,
                    "error": None,
                    "chi2dof": None,
                    "iterations": [],
                }
                for job_id, seed in zip(self.job_ids, self.seeds)
            ],
        }

    def load_data(self):
        pass

    def copy_input(self):
        if not self.input_local_path:
            return

        def input(*path: PathLike) -> Path:
            return Path(self.config["job"]["path"]).joinpath(
                *self.input_local_path, *path
            )

        with open(input(Executor._file_res), "r") as in_res:
            in_data = json.load(in_res)
            # > check here if it's a warmup?
            for in_file in in_data["output_files"]:
                # > always skip log (& dat) files
                # > if warmup copy over also txt files
                # > for production, only take the weights (skip txt)
                if in_file in self.data["input_files"]:
                    continue  # already copied in the past
                shutil.copyfile(input(in_file), self._local(in_file))
                self.data["input_files"].append(in_file)

    # def _input_local(self, *path: PathLike) -> Path:
    #     return Path(self.config["job"]["path"]).joinpath(*self.input_local_path, *path)

    def write_runcard(self):
        runcard = self._local(Executor._file_run)
        # > tempalte variables: {sweep, run, channels, channels_region, toplevel}
        channel_string = self.config["process"]["channels"][self.channel]["string"]
        if "region" in self.config["process"]["channels"][self.channel]:
            channel_region = self.config["process"]["channels"][self.channel]["region"]
        else:
            channel_region = ""
        dokan.runcard.fill_template(
            runcard,
            self.config["job"]["template"],
            sweep=f"{self.mode!s} = {self.ncall}[{self.niter}]",
            run="",
            channels=channel_string,
            channels_region=channel_region,
            toplevel="",
        )
        self.data["input_files"].append(Executor._file_run)

    @staticmethod
    def factory(policy=ExecutionPolicy.LOCAL, *args, **kwargs):
        if policy == ExecutionPolicy.LOCAL:
            # > local import to avoid cyclic dependence
            from ._local import LocalExec

            return LocalExec(policy, *args, **kwargs)
        # if policy == ExecutionPolicy.HTCONDOR: return HTCondorExec(*args, **kwargs)
        raise TypeError(f"invalid ExecutionPolicy: {policy!r}")

    # @staticmethod
    # def compute_md5(file_name: str):
    #   hash_md5 = hashlib.md5()
    #   with open(file_name, "rb") as f:
    #     for chunk in iter(lambda: f.read(4096), b""):
    #       hash_md5.update(chunk)
    #   return hash_md5.hexdigest()

    def requires(self):
        return []

    def output(self):
        return [luigi.LocalTarget(self._local(Executor._file_res))]

    @abstractmethod
    def exe(self) -> None:
        pass

    def run(self):
        # > copy over input files if there are any
        self.copy_input()
        # > generate a job runcard
        runcard = self._local(Executor._file_run)
        # > tempalte variables: {sweep, run, channels, channels_region, toplevel}
        channel_string = self.config["process"]["channels"][self.channel]["string"]
        if "region" in self.config["process"]["channels"][self.channel]:
            channel_region = self.config["process"]["channels"][self.channel]["region"]
        else:
            channel_region = ""
        dokan.runcard.fill_template(
            runcard,
            self.config["job"]["template"],
            sweep="{} = {}[{}]".format(
                ExecutionMode(self.exe_mode), self.ncall, self.niter
            ),
            run="",
            channels=channel_string,
            channels_region=channel_region,
            toplevel="",
        )
        self.data["input_files"].append(Executor._file_run)
        # > set the start time
        self.data["timestamp"] = time()
        # > more preparation for execution?
        # > check if job files are already there? (recovery mode?)
        self.exe()
        # > after exe done, generate the output file.
        shutil.move(self._local(Executor._file_tmp), self._local(Executor._file_res))
