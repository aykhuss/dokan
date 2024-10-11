"""data format for an `Executor`

We use a custom dictionary class to define the data structure
for the NNLOJET execution (`Executor`). This class also manages the
mutability and implements an atomic copy from temporary files.

Attributes
----------
_scheme : dict
    define the structure of ExeData
"""

from collections import UserDict
import json
from pathlib import Path
from os import PathLike

from . import ExecutionMode, ExecutionPolicy

# > deifne our own schema:
# list -> expect arbitrary number of entries with all the same type
# tuple -> expect list with exact number & types
# both these cases map to tuples as JSON only has lists
_schema: dict = {
    "mode": ExecutionMode,
    "policy": ExecutionPolicy,
    "policy_settings": {
        # --- LOCAL
        "local_ncores": int,
        # --- HTCONDOR
        "htcondor_id": int,
    },
    "ncall": 0,
    "niter": 0,
    # ---
    "timestamp": float,
    "input_files": [str],
    "output_files": [str],
    "results": [
        {
            "elapsed_time": float,
            "result": float,
            "error": float,
            "chi2dof": float,
            "iterations": [
                {
                    "result": float,
                    "error": float,
                    "chi2dof": float,
                }
            ],
        }
    ],
}


class ExeData(UserDict):
    # > class-local variables for file name conventions
    _file_tmp: str = "job.tmp"
    _file_fin: str = "job.json"

    def __init__(self, path: PathLike, *args, **kwargs):
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
        self._tmp: Path = self.path / self._file_tmp
        self._fin: Path = self.path / self._file_fin
        # > load in order of precedence & set mutable state
        self.load()

    def is_valid(self, struct=None, schema=_schema):
        if struct is None:
            return self.is_valid(self.data, schema)
        if isinstance(struct, dict) and isinstance(schema, dict):
            # struct is a dict of types or other dicts
            return all(
                k in schema and self.is_valid(struct[k], schema[k]) for k in struct
            )
        if isinstance(struct, list) and isinstance(schema, list):
            # struct is list in the form [type or dict]
            return all(self.is_valid(struct[0], c) for c in schema)
        # @todo: case for a tuple -> list with fixed length & types
        elif isinstance(schema, type):
            # struct is the type of schema
            return isinstance(struct, schema)
        else:
            # struct is neither a dict, nor list, not type
            return False

    def __setitem__(self, key, item) -> None:
        if not self._mutable:
            raise RuntimeError("ExeData is not in a mutable state!")
        super().__setitem__(key, item)
        if not self.is_valid():
            raise ValueError(f"ExeData scheme forbids: {key} : {item}")

    def load(self):
        self._mutable = True
        if self._fin.exists():
            with open(self._fin, "rt") as fin:
                self.data = json.load(fin)
                self._mutable = False
            if self._tmp.exists():
                raise RuntimeError(f"ExeData: tmp & fin exist {self._tmp}!")
        elif self._tmp.exists():
            with open(self._tmp, "rt") as tmp:
                self.data = json.load(tmp)
        if not self.is_valid():
            raise RuntimeError("ExeData load encountered conflict with schema")

    def write(self):
        with open(self._tmp, "w") as tmp:
            json.dump(self.data, tmp, indent=2)

    @property
    def mutable(self) -> bool:
        return self._mutable
