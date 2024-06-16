"""NNLOJET execution

module that defines all ways of executing NNLOJET (platforms, modes, ...)
"""

from enum import IntEnum, unique
from abc import ABCMeta, abstractmethod
import luigi
import shutil
import os
import json
from ..task import Task
import dokan.runcard
# import hashlib


@unique
class ExecutionPolicy(IntEnum):
    """policies on how/where to execute NNLOJET
    """

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
        """method for `argparse`
        """
        try:
            return ExecutionPolicy[s.upper()]
        except KeyError:
            return s


@unique
class ExecutionMode(IntEnum):
    """The two execution modes of NNLOJET
    """
    WARMUP = 1
    PRODUCTION = 2

    def __str__(self):
        return self.name.lower()


class Executor(Task, metaclass=ABCMeta):

    #> define some class-local variables for file name conventions
    _file_run = "job.run"
    _file_res = "job.json"
    _file_tmp = "job.tmp"
    _file_out = "job.out"
    _file_err = "job.err"

    #> if there's a previous warmup job pass the dir to it:
    #> need to get the `output_files` from there and copy over
    channel = luigi.Parameter()
    input_local_path: list = luigi.ListParameter(default=[])
    exe_mode: int = luigi.IntParameter()
    ncores: int = luigi.IntParameter(default=1)
    ncall: int = luigi.IntParameter()
    niter: int = luigi.IntParameter()
    iseed: int = luigi.IntParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._result = {
            "local_path": self.local_path,
            "channel": self.channel,
            "ncores": self.ncores,
            "exe_mode": self.exe_mode,
            "ncall": self.ncall,
            "niter": self.niter,
            "iseed": self.iseed,
            "input_files": [],
            "output_files": [],
            "iterations": [],
        }

    def _input_local(self, *path):
        return os.path.join(self.config["job"]["path"], *self.input_local_path,
                            *path)

    @staticmethod
    def factory(policy=ExecutionPolicy.LOCAL, *args, **kwargs):
        if policy == ExecutionPolicy.LOCAL: return LocalExec(*args, **kwargs)
        # if policy == ExecutionPolicy.HTCONDOR: return HTCondorExec(*args, **kwargs)
        raise TypeError("invalid ExecutionPolicy")

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
        if len(self.input_local_path) > 0:
            print("Executor: copy files from {}".format(self.input_local_path))
            with open(self._input_local(Executor._file_res), 'r') as wfile:
                wconfig = json.load(wfile)
                #> check here if it's even an warmup?
                for in_file in wconfig["output_files"]:
                    #> always skip log (& dat) files
                    #> if warmup copy over also txt files
                    #> for production, only take the weights (skip txt)
                    if in_file in self._result["input_files"]:
                        continue  # already copied in the past
                    shutil.copyfile(self._input_local(in_file),
                                    self._local(in_file))
                    self._result["input_files"].append(in_file)
        else:
            print("<<< brand new warmup >>>")
        runcard = self._local(Executor._file_run)
        #> tempalte variables: {sweep, run, channels, channels_region, toplevel}
        channel_string = self.config['process']['channels'][
            self.channel]['string']
        if 'region' in self.config['process']['channels'][self.channel]:
            channel_region = self.config['process']['channels'][
                self.channel]['region']
        else:
            channel_region = ''
        dokan.runcard.fill_template(runcard,
                                    self.config["job"]["template"],
                                    sweep='{} = {}[{}]'.format(
                                        ExecutionMode(self.exe_mode),
                                        self.ncall, self.niter),
                                    run='',
                                    channels=channel_string,
                                    channels_region=channel_region,
                                    toplevel='')
        self._result["input_files"].append(Executor._file_run)
        #> more preparation for execution?
        #> check if job files are already there? (recovery mode?)
        self.exe()
        #> after exe done, generate the output file.
        shutil.move(self._local(Executor._file_tmp),
                    self._local(Executor._file_res))

    def output(self):
        return luigi.LocalTarget(self._local(Executor._file_res))


from dokan.exe._local import LocalExec
