# SPDX-FileCopyrightText: © 2024-present NNLOJET
#
# SPDX-License-Identifier: MIT

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
from ..task import Task
import dokan.runcard
# import hashlib


@unique
class ExecutionPolicy(IntEnum):
    """policies on how/where to execute NNLOJET"""

    NULL = 0
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

    WARMUP = 1
    PRODUCTION = 2

    def __str__(self):
        return self.name.lower()


# > give all needed parameters for an NNLOcalculation explicitly as parameters?
class Executor(Task, metaclass=ABCMeta):
    # > define some class-local variables for file name conventions
    _file_run: str = "job.run"
    _file_res: str = "job.json"
    _file_tmp: str = "job.tmp"

    # > if there's a previous wzarmup job pass the dir to it:
    # > need to get the `output_files` from there and copy over
    job_ids: list[int] = luigi.ListParameter()
    seeds: list[int] = luigi.ListParameter()
    policy: int = luigi.OptionalIntParameter(default=ExecutionPolicy.NULL)
    mode: int = luigi.IntParameter()
    input_local_path: list = luigi.ListParameter(default=[])  # warmup we need
    ncall: int = luigi.IntParameter()
    niter: int = luigi.IntParameter()

    # @todo: add options for overrides here?

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # > create data structure for the executor
        # @todo abstract away this datastructure to something more rigid like CONFIG?
        self._data = {
            "job_ids": self.job_ids,
            "seeds": self.seeds,
            "local_path": self.local_path,
            "policy": self.policy,
            "policy_settings": {},
            "mode": self.mode,
            "ncall": self.ncall,
            "niter": self.niter,
            "input_files": [],
            "output_files": [],
            "iterations": [],
        }
        # > some consistency checks
        if len(self.job_ids) != len(self.seeds):
            raise ValueError("Executor: job_ids and seeds must have the same length")
        if (len(self.seeds) > 1) and all(
            self.seeds[i] + 1 == self.seeds[i + 1] for i in range(len(self.seeds) - 1)
        ):
            raise ValueError(
                f"Executor: seeds must be a consecutive list! {self.seeds}"
            )

    def _input_local(self, *path: PathLike) -> Path:
        return Path(self.config["job"]["path"]).joinpath(*self.input_local_path, *path)

    @staticmethod
    def factory(policy=ExecutionPolicy.LOCAL, *args, **kwargs):
        if policy == ExecutionPolicy.LOCAL:
            # > local import to avoid cyclic dependence
            from ._local import LocalExec

            return LocalExec(*args, **kwargs)
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

    @abstractmethod
    def exe(self):
        pass

    def run(self):
        #> copy over input files
        if self.input_local_path:
            print("Executor: copy files from {}".format(self.input_local_path))
            with open(self._input_local(Executor._file_res), "r") as wfile:
                wconfig = json.load(wfile)
                # > check here if it's even an warmup?
                for in_file in wconfig["output_files"]:
                    # > always skip log (& dat) files
                    # > if warmup copy over also txt files
                    # > for production, only take the weights (skip txt)
                    if in_file in self._result["input_files"]:
                        continue  # already copied in the past
                    shutil.copyfile(self._input_local(in_file), self._local(in_file))
                    self._result["input_files"].append(in_file)
        else:
            print("<<< brand new warmup >>>")
        #> generate a job runcard
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
        self._result["input_files"].append(Executor._file_run)
        # > more preparation for execution?
        # > check if job files are already there? (recovery mode?)
        self.exe()
        # > after exe done, generate the output file.
        shutil.move(self._local(Executor._file_tmp), self._local(Executor._file_res))

    def output(self):
        return [luigi.LocalTarget(self._local(Executor._file_res))]
