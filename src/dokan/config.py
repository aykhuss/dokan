
"""configuration for the dokan workflow.

We use a custom dictionary class to store all settings that we need to exeucute
a full NNLOJET workflow.

Attributes
----------
_default_config : Path
    path to a configuration file with default values
_attributes : dict
    allowed structures and keys for _Config
CONFIG : _Config
    a global configuration object that stores the current state of the workflow
    settings. Note that this is not accessible by the Tasks once they are
    dispatched, which is why we need to pass it along as a class parameter in
    the dokan tasks.
"""

from collections import UserDict
import json
from pathlib import Path
from os import PathLike

_default_config: Path = Path(__file__).parent.resolve() / "config.json"

_attributes: dict = {
    "exe": (
        "path",  # absolute path to NNLOJET
        "policy",  # how to execute (local, htcondor, slurm, ...)
    ),
    "job": (
        "name",  # job name
        "path",  # absolute path to job directory
        "template",  # absolute path to the template file
        "order",  # what order to comput (LO, NLO, NNLO)
        "target_rel_acc",  # target relative accuracy
        "max_runtime",  # maximum runtime (in sec) for a single NNLOJET run
        "max_total",  # mmaximum number of total (production?) jobs
        "max_concurrent",  # maximum number of concurrent jobs
        "batch_size",  # @todo: size of runs to batch
        "seed_offset",  # seed number offset
        "timestamps",  # list of timestamps when `run` was called
    ),
    "process": (
        "name",  # name of the process in NNLOJET
        "channels",  # all channels for the process (auto-filled)
    ),
    "warmup": (
        "ncores",  # #of cores to allocate to a single warmup run
        "ncall_start",  # initial number of events (per iteration)
        "niter",  # number of iterations in a single job (>=2 for chi2dof)
        "max_increment",  # up to how many rounds of warmups we want to run
        "fac_increment",  # the factor by which we increment the statistics each round
    ),
    "production": (
        "ncores",  # #of cores to allocate to a single production run
        "ncall_start",  # initial number of events (per iteration)
        "niter",  # number of iterations in a single job (>=2 for chi2dof)
    ),
}


class _Config(UserDict):
    """configuration class of the dokan workflow

    a custon dictionary with a rigid skeleton to store workflow settings.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # > create the section skeleton
        for sect in _attributes:
            if sect not in self.data:
                self.data[sect] = {}
        self.load_config()

    def __setitem__(self, key, item) -> None:
        raise RuntimeError("use setter routines for modifying config")

    def _get_config_path(self, must_exist: bool = True) -> Path:
        """get path to the configuration file.

        if the configuration already knows of a path, return it (depending on the must_exist flag).
        Otherwise return the default config file.

        Parameters
        ----------
        must_exist : bool, optional
            flag to enforce the existence of the config file store in the configuration.
            (the default is True, which enforces that the config file actually exists)

        Returns
        -------
        Path
            the path to the donfiguration file
        """
        config_path = _default_config
        if "path" in self.data["job"]:
            job_config_path = Path(self.data["job"]["path"]) / "config.json"
            if (must_exist and job_config_path.exists()) or not must_exist:
                config_path = job_config_path
        return config_path

    def load_config(self, default_ok: bool = True):
        """load configuration from file

        load the settings for the configuration from a config file.
        initially, we'll allow it to be the default config to populate the settings,
        later, we'll want to insist there to be a job-specific configuration file.

        Parameters
        ----------
        default_ok : bool, optional
            flag to allow default config to be loaded of job-specific one does not exist.
            (the default is True, which allows to load the default config)

        Raises
        ------
        RuntimeError
            error when we insist on a job-specific configuration but if it does not exist.
        """
        in_config_path = self._get_config_path(must_exist=True)
        if (in_config_path == _default_config) and not default_ok:
            raise RuntimeError("default_ok=False but tried to read default config")
        with open(in_config_path, "rt") as f:
            in_config = json.load(f)
        for sect in _attributes:
            if sect not in self.data:
                self.data[sect] = {}
            if sect in in_config:
                for key, value in in_config[sect].items():
                    if key not in _attributes[sect]:
                        print(f"skipping {sect} entry {key}")
                        continue
                    if sect == "job" and key == "path":
                        continue  # this is set at runtime and should not be cached?
                    self.data[sect][key] = value

    def write_config(self):
        """write configuration to file

        take the current settings and write them to the configuration file in the job folder.

        Raises
        ------
        RuntimeError
            error when attempting to overwrite default config (should never get here)
        """
        out_config_path = self._get_config_path(must_exist=False)
        if out_config_path == _default_config:
            raise RuntimeError("can't overwrite default config file")
        with open(out_config_path, "w") as f:
            json.dump(self.data, f, indent=2)

    @property
    def process(self):
        return self.data["process"]

    def set_process(self, **kwargs):
        for key in kwargs:
            if key not in _attributes["process"]:
                raise AttributeError(f"invalid process attribute: {key}")
            self.data["process"][key] = kwargs[key]

    @property
    def exe(self):
        return self.data["exe"]

    def set_exe(self, **kwargs):
        for key in kwargs:
            if key not in _attributes["exe"]:
                raise AttributeError(f"invalid exe attribute: {key}")
            self.data["exe"][key] = kwargs[key]

    @property
    def job(self):
        return self.data["job"]

    def set_job(self, **kwargs):
        for key in kwargs:
            if key not in _attributes["job"]:
                raise AttributeError(f"invalid job attribute: {key}")
            if key == "path":
                self.set_job_path(kwargs[key])
            else:
                self.data["job"][key] = kwargs[key]

    @property
    def job_path(self) -> Path:
        if "path" not in self.data["job"]:
            raise AttributeError("job path is not set")
        return Path(self.data["job"]["path"])

    def set_job_path(self, path: PathLike):
        job_path = Path(path)
        if not job_path.exists():
            job_path.mkdir(parents=True)
        self.data["job"]["path"] = str(job_path.absolute())
        self.set_job(template=str((job_path / "template.run").relative_to(job_path)))

    # def set_test_mode_enabled(self, val: bool):
    #     if isinstance(val, bool):
    #         os.environ['HW_TEST_MODE_ENABLED'] = str(val).upper()
    #     else:
    #         raise TypeError(f"Must provide a bool, not a {type(val)}")


CONFIG: _Config = _Config()
