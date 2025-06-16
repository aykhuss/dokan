"""configuration for the dokan workflow.

We use a custom dictionary class to store all settings that we need to exeucute
a full NNLOJET workflow.

Attributes
----------
_default_config : Path to config file that stores default values
    path to a configuration file with default values
_schema : dict
    define the structure of Config
"""

import json
from collections import UserDict
from pathlib import Path

from ._types import GenericPath
from .db._loglevel import LogLevel
from .exe import ExecutionPolicy
from .order import Order
from .runcard import RuncardTemplate
from .util import fill_missing, validate_schema

_default_config: Path = Path(__file__).parent.resolve() / "config.json"

_schema: dict = {
    "exe": {
        "path": str,  # absolute path to NNLOJET
        "policy": ExecutionPolicy,  # (local, htcondor, slurm, ...)
        "policy_settings": {
            # --- LOCAL
            "local_ncores": int,
            # --- HTCONDOR
            "htcondor_template": str,
            "htcondor_ncores": int,
            "htcondor_nretry": int,
            "htcondor_retry_delay": float,
            "htcondor_poll_time": float,
            # --- SLURM
            "slurm_template": str,
            "slurm_ncores": int,
            "slurm_nretry": int,
            "slurm_retry_delay": float,
            "slurm_poll_time": float,
            "slurm_njobs_per_node": int,
        },
    },
    "run": {
        "dokan_version": str,  # verion of the workflow
        "name": str,  # job name
        "path": str,  # absolute path to job directory
        "raw_path": str,  # (optional) absolute path to raw data directory
        "template": str,  # template file name (not path)
        "md5": str,  # hash of the template file
        "histograms": {str: {"nx": int, "cumulant": int, "grid": str}},  # list of all histograms
        "histograms_single_file": str,  # name in case we concatenate all histograms to a single file
        "order": Order,  # what order to compute (LO, NLO, NNLO)
        "opt_target": str,  # the target we wish to optimise: ["cross"|"cross_hist"|"hist"]
        "target_rel_acc": float,  # target relative accuracy
        "job_max_runtime": float,  # maximum runtime (in sec) for a single NNLOJET run
        "job_fill_max_runtime": bool,  # if we want to exhause the maximum runtime
        "jobs_max_total": int,  # maximum number of total (production?) jobs
        "jobs_max_concurrent": int,  # maximum number of concurrent jobs
        "jobs_batch_size": int,  # size of runs to batch into a single submission
        "jobs_batch_unit_size": int,  # the minimum batch size of a submission
        "seed_offset": int,  # seed number offset
        "timestamps": float,  # @todo list of timestamps when `run` was called
    },
    "ui": {
        "monitor": bool,
        "refresh_delay": float,
        "log_level": LogLevel,
    },
    "process": {
        "name": str,  # name of the process in NNLOJET
        "channels": {
            str: {
                "string": str,
                "part": str,
                "part_num": int,
                "region": str,
                "order": int,
            },
        },  # all channels for the process (auto-filled)
    },
    "warmup": {
        "ncores": int,  # #of cores to allocate to a single warmup run
        "ncall_start": int,  # initial number of events (per iteration)
        "niter": int,  # number of iterations in a single job (>=2 for chi2dof)
        "min_increment_steps": int,  # must be > 2 and < max value
        "max_increment_steps": int,  # up to how many rounds of warmups we want to run
        "fac_increment": float,  # the factor by which we increment the statistics each round
        "max_chi2dof": float,
        "max_err_rel_var": float,
        "scaling_window": float,
    },
    "production": {
        "ncores": int,  # #of cores to allocate to a single production run
        "ncall_start": int,  # initial number of events (per iteration)
        "niter": int,  # number of iterations in a single job (>=2 for chi2dof)
        "penalty_wrt_warmup": float,  # factor that takes into account the slowdown from warmup -> production
        "fac_merge_trigger": float,  # factor that triggers a merge if ((#done+#merged)/(#merged+1)) > fac_merge_trigger
        "min_number": int,  # minimum #of production jobs beyond pre-production (defaults to 1)
    },
    "merge": {
        "trim_threshold": float,  # threshold to trim outliers
        "trim_max_fraction": float,  # maximum fraction to trim (dynamically adjust threshod to satisfy)
        "k_scan_nsteps": int,  # number of scan steps to consider for finding the plateau
        "k_scan_maxdev_steps": float,  # maximum deviation to identify a plateau
    },
}


class Config(UserDict):
    """configuration class of the dokan workflow

    a custom dictionary with a rigid skeleton to store workflow settings.
    """

    # > class-local variables for file name conventions
    _file_cfg: str = "config.json"

    def __init__(self, *args, **kwargs):
        path = kwargs.pop("path", None)
        default_ok: bool = kwargs.pop("default_ok", True)
        super().__init__(*args, **kwargs)
        self.path: Path = None
        self.file_cfg: Path = None
        if path:
            if not default_ok:
                self.set_path(path, load=True)
            else:
                self.load(default_ok)
                self.set_path(path, load=False)
        else:
            self.load(default_ok)
        # > ensure that missing entries are always filled with defaults
        self.fill_defaults()

    def is_valid(self, convert_to_type: bool = False) -> bool:
        if not validate_schema(self.data, _schema, convert_to_type):
            return False
        # > implement boundary conditions on the configuration here
        # > that goes beyond the schema (structure and types)
        if "run" in self.data:
            if "target_rel_acc" in self.data["run"] and self.data["run"]["target_rel_acc"] <= 0.0:
                return False
            if "seed_offset" in self.data["run"] and self.data["run"]["seed_offset"] < 0:
                return False
        if "warmup" in self.data:
            if (
                "min_increment_steps" in self.data["warmup"]
                and self.data["warmup"]["min_increment_steps"] < 2
            ):
                return False
        if "production" in self.data:
            if (
                "min_number" in self.data["production"]
                and self.data["production"]["min_number"] < 1
            ):
                return False
        return True

    def __setitem__(self, key, item) -> None:
        super().__setitem__(key, item)
        if not self.is_valid():
            raise ValueError(f"Config: scheme forbids: {key} : {item}")

    def set_path(self, path: GenericPath, load: bool = False) -> None:
        self.path = Path(path)
        if not self.path.exists():
            self.path.mkdir(parents=True)
        if not self.path.is_dir():
            raise ValueError(f"{path} is not a folder")
        self.file_cfg = self.path / self._file_cfg
        if load:
            self.load(default_ok=False)
        self["run"]["path"] = str(self.path.absolute())

    def load_defaults(self) -> None:
        with open(_default_config, "rt") as tmp:
            self.data = json.load(tmp)
        if not self.is_valid(convert_to_type=True):
            raise RuntimeError("Config: load_defaults encountered conflict with schema")

    def load(self, default_ok: bool = True) -> None:
        if self.file_cfg and self.file_cfg.exists():
            with open(self.file_cfg, "rt") as fin:
                self.data = json.load(fin)
        else:
            if not default_ok:
                raise FileNotFoundError(f"Config file not found: {self.file_cfg}")
            self.load_defaults()
        if not self.is_valid(convert_to_type=True):
            raise RuntimeError("Config: load encountered conflict with schema")

        # > Check whether the template file matches the md5 entry
        if self.path is not None and self.data.get("run", {}).get("template") is not None:
            template_hash = RuncardTemplate(self.path / self.data["run"]["template"]).to_md5_hash()
            # > skip the check if we don't have a md5 yet
            if (original_hash := self.data["run"].get("md5")) is not None:
                if template_hash != original_hash:
                    raise RuntimeError("Template has been manually modified, this is not allowed.")

    def fill_defaults(self):
        with open(_default_config, "rt") as tmp:
            defaults = json.load(tmp)
            fill_missing(self.data, defaults)

    def write(self) -> None:
        if not self.path or not self.file_cfg:
            raise RuntimeError("Config: no path set?!")
        with open(self.file_cfg, "w") as cfg:
            json.dump(self.data, cfg, indent=2)
