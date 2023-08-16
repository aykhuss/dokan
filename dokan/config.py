"""configuration for a dokan job

We define an internal dictionary class to handle all settings that are
needed to execute a full NNLOJET workflow.

Attributes:
    CONFIG (_Config): an instance in global scope
"""

from collections import UserDict
import os
import json

_default_config = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                               "config.json")

_attributes = {
    "exe": (
        "path",  # absolute path to NNLOJET
        "policy"),  # how to execute (local, htcondor, slurm, ...)
    "job": (
        "path",  # absolute path to job directory
        "name",  # job name
        "template",  # absolute path to the template file
        "order",  # what order to comput (LO, NLO, NNLO)
        "min_runtime",  # minimum runtime (in sec) for a single NNLOJET run
        "max_runtime",  # maximum runtime (in sec) for a single NNLOJET run
        "batch_size",  #@todo: size of runs to batch
        "iseed_start"  # seed number offset
    ),
    "process": (
        "name",  # name of the process in NNLOJET
        "channels"  # all channels for the process (auto-filled)
    ),
    "warmup": (
        "ncores",  # #of cores to allocate to a single warmup run
        "ncall_start",  # initial number of events (per iteration)
        "niter",  # number of iterations in a single job (>=2 for chi2dof)
        "max_increment",  # up to how many rounds of warmups we want to run
        "fac_increment"  # the factor by which we increment the statistics each round
    ),
    "production": (
        "ncores",  # #of cores to allocate to a single production run
        "ncall_start",  # initial number of events (per iteration)
        "niter"  # number of iterations in a single job (>=2 for chi2dof)
    )
}


class _Config(UserDict):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        #> create the section skeleton
        for sect in _attributes:
            if sect not in self.data:
                self.data[sect] = {}
        self.load_config()

    def __setitem__(self, key, item) -> None:
        raise RuntimeError("use setter routines for modifying config")

    def _get_config_path(self, must_exist=True) -> str:
        config_path = _default_config
        if "path" in self.data["job"]:
            job_config_path = os.path.join(self.data["job"]["path"],
                                           "config.json")
            if (must_exist
                    and os.path.exists(job_config_path)) or not must_exist:
                config_path = job_config_path
        return config_path

    def load_config(self, default_ok=True):
        in_config_path = self._get_config_path(must_exist=True)
        if (in_config_path == _default_config) and not default_ok:
            raise RuntimeError(
                "default_ok=False but tried to read default config")
        with open(in_config_path, 'rt') as f:
            in_config = json.load(f)
        for sect in _attributes:
            if sect not in self.data:
                self.data[sect] = {}
            if sect in in_config:
                for key, value in in_config[sect].items():
                    if key not in _attributes[sect]:
                        print("skipping {} entry {}".format(sect, key))
                        continue
                    if sect == "job" and key == "path":
                        continue  # this is set at runtime and should not be cached?
                    self.data[sect][key] = value

    def write_config(self):
        out_config_path = self._get_config_path(must_exist=False)
        if out_config_path == _default_config:
            raise RuntimeError("can't overwrite default config file")
        with open(out_config_path, 'w') as f:
            json.dump(self.data, f, indent=2)

    @property
    def process(self):
        return self.data['process']

    def set_process(self, **kwargs):
        for key in kwargs:
            if key not in _attributes['process']:
                raise AttributeError(f"invalid process attribute: {key}")
            self.data['process'][key] = kwargs[key]

    @property
    def exe(self):
        return self.data['exe']

    def set_exe(self, **kwargs):
        for key in kwargs:
            if key not in _attributes['exe']:
                raise AttributeError(f"invalid exe attribute: {key}")
            self.data['exe'][key] = kwargs[key]

    @property
    def job(self):
        return self.data['job']

    def set_job(self, **kwargs):
        for key in kwargs:
            if key not in _attributes['job']:
                raise AttributeError(f"invalid job attribute: {key}")
            if key == 'path':
                self.set_job_path(kwargs[key])
            else:
                self.data['job'][key] = kwargs[key]

    @property
    def job_path(self):
        if not "path" in self.data["job"]:
            raise AttributeError(f"job path is not set")
        return self.data["job"]["path"]

    def set_job_path(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
        #> `normpath` to remove trailing `/`
        self.data["job"]["path"] = os.path.normpath(os.path.abspath(path))
        self.set_job(
            template=os.path.join(self.data["job"]["path"], "template.run"))

    # def set_test_mode_enabled(self, val: bool):
    #     if isinstance(val, bool):
    #         os.environ['HW_TEST_MODE_ENABLED'] = str(val).upper()
    #     else:
    #         raise TypeError(f"Must provide a bool, not a {type(val)}")


CONFIG = _Config()
