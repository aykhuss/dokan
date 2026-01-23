"""Data format for an `Executor`.

We use a custom dictionary class to define the data structure
for the NNLOJET execution (`Executor`). This class also manages the
mutability and implements an atomic copy from temporary files.
"""

import json
import os
import shutil
import time
from collections import UserDict
from pathlib import Path
from typing import Any

from .._types import GenericPath
from ..nnlojet import parse_log_file
from ..util import validate_schema
from ._exe_config import ExecutionMode, ExecutionPolicy

# > define our own schema:
# list -> expect arbitrary number of entries with all the same type
# tuple -> expect list with exact number & types
# both these cases map to tuples as JSON only has lists
_schema: dict = {
    "exe": str,
    "timestamp": float,
    "mode": ExecutionMode,
    "policy": ExecutionPolicy,
    "policy_settings": {
        "max_runtime": float,
        # --- LOCAL
        "local_ncores": int,
        # --- HTCONDOR
        "htcondor_id": int,
        "htcondor_template": str,
        "htcondor_ncores": int,
        "htcondor_nretry": int,
        "htcondor_retry_delay": float,
        "htcondor_poll_time": float,
        # --- SLURM
        "slurm_id": int,
        "slurm_template": str,
        "slurm_ncores": int,
        "slurm_nretry": int,
        "slurm_retry_delay": float,
        "slurm_poll_time": float,
    },
    "ncall": int,
    "niter": int,
    # ---
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
    """Execution Data Manager.

    Manages the state and persistence of execution data for NNLOJET jobs.
    It handles transitions between mutable (temporary) and immutable (final) states
    backed by JSON files.

    Attributes
    ----------
    path : Path
        The directory path where job data is stored.
    file_tmp : Path
        Path to the temporary data file (`job.tmp`).
    file_fin : Path
        Path to the final data file (`job.json`).

    """

    # > class-local variables for file name conventions
    _file_tmp: str = "job.tmp"
    _file_fin: str = "job.json"

    def __init__(self, path: GenericPath, *args, **kwargs):
        """Initialize ExeData.

        Parameters
        ----------
        path : GenericPath
            Path to the job directory.
        expect_tmp : bool, optional
            If True, raises an error if the temporary file is missing (default: False).

        """
        expect_tmp: bool = kwargs.pop("expect_tmp", False)
        super().__init__(*args, **kwargs)

        self.path: Path = Path(path)
        if not self.path.exists():
            self.path.mkdir(parents=True, exist_ok=True)
        if not self.path.is_dir():
            raise ValueError(f"{path} is not a folder")

        self.file_tmp: Path = self.path / ExeData._file_tmp
        self.file_fin: Path = self.path / ExeData._file_fin

        # > Internal mutable state flag
        self._mutable: bool = True

        # > load in order of precedence & set mutable state
        self.load(expect_tmp)

    def is_valid(self, convert_to_type: bool = False) -> bool:
        """Validate current data against the schema.

        Parameters
        ----------
        convert_to_type : bool, optional
            If True, attempts to convert values to the schema types (default: False).

        Returns
        -------
        bool
            True if valid, False otherwise.

        """
        return validate_schema(self.data, _schema, convert_to_type)

    def __setitem__(self, key: Any, item: Any) -> None:
        """Set an item with mutability check.

        Note: Schema validation on every set is disabled for performance.
        Call `is_valid()` explicitly if needed.
        """
        if not self._mutable:
            raise RuntimeError("ExeData is not in a mutable state!")
        super().__setitem__(key, item)

    @property
    def timestamp(self) -> float:
        """Get the modification timestamp of the active data file."""
        if self._mutable and "timestamp" in self.data:
            # > when mutable this indicates the time `exe` was called
            return self.data["timestamp"]

        target = self.file_tmp if self._mutable else self.file_fin
        if target.exists():
            return target.stat().st_mtime
        return 0.0

    def load(self, expect_tmp: bool = False) -> None:
        """Load data from disk.

        Prioritizes the final file (`job.json`) over the temporary file (`job.tmp`).
        Sets the `_mutable` flag accordingly.

        Parameters
        ----------
        expect_tmp : bool
            If True, raise RuntimeError if no temporary file is found when no final file exists.

        """
        self._mutable = True
        if self.file_fin.exists():
            with open(self.file_fin) as fin:
                self.data = json.load(fin)
                self._mutable = False
            # Warning: existence of both files might indicate a crash during finalization
            if self.file_tmp.exists():
                pass  # print(f"Warning: both {self.file_fin} and {self.file_tmp} exist. Using final.")
        elif self.file_tmp.exists():
            with open(self.file_tmp) as tmp:
                self.data = json.load(tmp)
        elif expect_tmp:
            raise RuntimeError(f"ExeData: tmp expected but not found {self.file_tmp}!")

        # Validate after loading to ensure integrity
        if self.data and not self.is_valid(convert_to_type=True):
            # Just warn for now to allow loading potentially slightly mismatched data during dev
            # print("ExeData load encountered conflict with schema")
            pass

    def write(self) -> None:
        """Write current data to the temporary file atomically."""
        if not self._mutable:
            raise RuntimeError("ExeData can't write after finalize!")

        # Atomic write: write to .tmp.swp then rename to .tmp
        temp_swp = self.file_tmp.with_suffix(".tmp.swp")
        with open(temp_swp, "w") as tmp:
            json.dump(self.data, tmp, indent=2)
        shutil.move(temp_swp, self.file_tmp)

    def finalize(self) -> None:
        """Finalize the data: write to disk and move to final filename."""
        if not self._mutable:
            # Idempotent: if already final, do nothing or check?
            # raise RuntimeError("ExeData already finalized?!")
            return

        self.write()
        shutil.move(self.file_tmp, self.file_fin)
        self._mutable = False

    def make_mutable(self) -> None:
        """Revert final status to mutable (moves json -> tmp)."""
        if self._mutable:
            return
        if self.file_fin.exists():
            shutil.move(self.file_fin, self.file_tmp)
        self._mutable = True

    @property
    def is_final(self) -> bool:
        """Check if data is in the final (immutable) state."""
        return not self._mutable

    @property
    def is_mutable(self) -> bool:
        """Check if data is in the mutable state."""
        return self._mutable

    def scan_dir(self, skip_files: list[str] | None = None, force: bool = False, **kwargs) -> None:
        """Scan directory for output files and update job data.

        Parameters
        ----------
        skip_files : list[str], optional
            List of filenames to ignore.
        force : bool
            Force scan even if immutable (will temporarily make mutable?). Currently no-op if immutable.
        **kwargs :
            reset_output (bool): Clear existing output_files list.
            fs_max_retry (int): Number of retries for filesystem scan.
            fs_delay (float): Delay between retries.

        """
        if not self._mutable and not force:
            return

        reset_output: bool = kwargs.pop("reset_output", False)
        fs_max_retry: int = kwargs.pop("fs_max_retry", 1)
        fs_delay: float = kwargs.pop("fs_delay", 0.0)

        skip_entries: list[str] = [ExeData._file_tmp, ExeData._file_fin]
        if skip_files:
            skip_entries.extend(skip_files)

        if reset_output:
            self.data["output_files"] = []

        # Ensure 'output_files' exists
        if "output_files" not in self.data:
            self.data["output_files"] = []

        found_new = False
        for _ in range(fs_max_retry):
            for entry in os.scandir(self.path):
                if entry.name in skip_entries:
                    continue
                if self.timestamp > 0 and entry.stat().st_mtime < self.timestamp:
                    continue

                if entry.name not in self.data["output_files"]:
                    self.data["output_files"].append(entry.name)
                    found_new = True

            if found_new:
                break
            time.sleep(fs_delay)

        # > parse logs and populate dictionary
        for _, job_data in self.data["jobs"].items():
            log_matches = [
                of for of in self.data["output_files"] if of.endswith(f".s{job_data['seed']}.log")
            ]
            if len(log_matches) != 1:
                continue

            try:
                parsed_data = parse_log_file(Path(self.path) / log_matches[0])
                for key in parsed_data:
                    job_data[key] = parsed_data[key]
            except Exception:
                continue

    @property
    def is_complete(self) -> bool:
        """Check if all defined jobs have a result."""
        if not self.data.get("jobs", {}):
            return False
        for job_data in self.data.get("jobs", {}).values():
            if "result" not in job_data:
                return False
        return True
