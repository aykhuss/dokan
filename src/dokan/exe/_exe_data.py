"""data format for an `Executor`

We use a custom dictionary class to define the data structure
for the NNLOJET execution (`Executor`). This class also manages the
mutability and implements an atomic copy from temporary files.

Attributes
----------
_schema : dict
    define the structure of ExeData
"""

from collections import UserDict
import json
import shutil
import time
from pathlib import Path
from os import PathLike


from ._exe_config import ExecutionMode, ExecutionPolicy
from ..util import validate_schema

# > deifne our own schema:
# list -> expect arbitrary number of entries with all the same type
# tuple -> expect list with exact number & types
# both these cases map to tuples as JSON only has lists
_schema: dict = {
    "exe": str,
    "mode": ExecutionMode,
    "policy": ExecutionPolicy,
    "policy_settings": {
        # --- LOCAL
        "local_ncores": int,
        # --- HTCONDOR
        "htcondor_id": int,
        "htcondor_ncores": int,
    },
    "ncall": int,
    "niter": int,
    # ---
    "timestamp": float,  # updated on each write
    "input_files": [str],  # first entry must be runcard?
    "output_files": [str],
    "jobs": {
        int: {
            # "job_id": int, # <-- now the key in a dict
            "seed": int,
            "elapsed_time": float,
            "result": float,  # job failure indicated by missing "result"
            "error": float,
            "chi2dof": float,
            "iterations": [
                {
                    "iteration": int,
                    "result": float,
                    "error": float,
                    "result_acc": float,
                    "error_acc": float,
                    "chi2dof": float,
                }
            ],
        }
    },
}


class ExeData(UserDict):
    # > class-local variables for file name conventions
    _file_tmp: str = "job.tmp"
    _file_fin: str = "job.json"

    def __init__(self, path: PathLike, *args, **kwargs):
        expect_tmp: bool = kwargs.pop("expect_tmp", False)
        # @todo: pass a path to a folder and automatically create a
        # tmp file if not existent, otherwise load file and go from there.
        # If final result file exists load that and make the thing immupable,
        # i.e. no changes allowed: bool flag? add an reset method to delete
        # the result file or to move the result file into a tmp file to
        # make it mutable again. finalize method to do an atomic my of
        # the file to the final state
        super().__init__(*args, **kwargs)
        # > check `path` and define files
        # @todo maybe allow files that match the file name conventions?
        self.path: Path = Path(path)
        if not self.path.exists():
            self.path.mkdir(parents=True)
        if not self.path.is_dir():
            raise ValueError(f"{path} is not a folder")
        self.file_tmp: Path = self.path / self._file_tmp
        self.file_fin: Path = self.path / self._file_fin
        # > load in order of precedence & set mutable state
        self.load(expect_tmp)

    def is_valid(self, convert_to_type: bool = False) -> bool:
        return validate_schema(self.data, _schema, convert_to_type)

    def __setitem__(self, key, item) -> None:
        if not self._mutable:
            raise RuntimeError("ExeData is not in a mutable state!")
        super().__setitem__(key, item)
        if not self.is_valid():
            raise ValueError(f"ExeData scheme forbids: {key} : {item}")

    def load(self, expect_tmp: bool = False) -> None:
        self._mutable = True
        if self.file_fin.exists():
            print(f"loading final file {self.file_fin}")
            with open(self.file_fin, "rt") as fin:
                self.data = json.load(fin)
                self._mutable = False
            if self.file_tmp.exists():
                raise RuntimeError(f"ExeData: tmp & fin exist {self.file_tmp}!")
        elif self.file_tmp.exists():
            print(f"loading temporary file {self.file_tmp}")
            with open(self.file_tmp, "rt") as tmp:
                self.data = json.load(tmp)
        elif expect_tmp:
            raise RuntimeError(f"ExeData: tmp expected but not found {self.file_tmp}!")
        if not self.is_valid(convert_to_type=True):
            raise RuntimeError("ExeData load encountered conflict with schema")
        print(f"ExeData::load: {self.data}")

    def write(self) -> None:
        if self._mutable:
            self.data["timestamp"] = time.time()
            with open(self.file_tmp, "w") as tmp:
                json.dump(self.data, tmp, indent=2)
        else:
            raise RuntimeError("ExeData can't write after finalize!")

    def finalize(self) -> None:
        if not self._mutable:
            raise RuntimeError("ExeData already finalized?!")
        self.write()
        shutil.move(self.file_tmp, self.file_fin)
        self._mutable = False

    def make_mutable(self) -> None:
        if self._mutable:
            return
        shutil.move(self.file_fin, self.file_tmp)
        self._mutable = True

    @property
    def is_final(self) -> bool:
        return not self._mutable

    @property
    def is_mutable(self) -> bool:
        return self._mutable
