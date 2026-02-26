"""Execution metadata container used by backend executors.

`ExeData` wraps the on-disk job metadata (`job.tmp` / `job.json`) and provides
helpers for scanning output directories, parsing per-seed logs, and managing
mutable vs. finalized state.
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
    "part_id": int,
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
    "lost_files": [str],
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

    Manages state and persistence of execution metadata for NNLOJET jobs.

    Storage lifecycle
    -----------------
    - Mutable state is stored in `job.tmp`.
    - Finalized state is stored in `job.json`.
    - `load()` prefers `job.json` when both files exist.

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

        Notes
        -----
        The target directory is created automatically when missing.

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
        if not self.is_valid(convert_to_type=True):
            raise ValueError(f"ExeData loaded data does not conform to schema at {path}!")

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
    def st_mtime(self) -> float:
        """Return `st_mtime` of the active on-disk metadata file."""
        target = self.file_tmp if self._mutable else self.file_fin
        if target.exists():
            return target.stat().st_mtime
        return 0.0

    @property
    def touch(self) -> float:
        """Update the active metadata file mtime and return the new value.

        The active file is `job.tmp` in mutable state and `job.json` in final
        state.
        """
        target = self.file_tmp if self._mutable else self.file_fin
        if not target.exists():
            raise RuntimeError(f"ExeData::touch: file does not exist: {target}")
        target.touch(exist_ok=True)
        return target.stat().st_mtime

    @property
    def timestamp(self) -> float:
        """Return the execution timestamp used for output-file filtering.

        In mutable state, `data["timestamp"]` (if present) takes precedence.
        Otherwise falls back to `st_mtime` of the active metadata file.
        """
        if self._mutable and "timestamp" in self.data:
            # > when mutable this indicates the time `exe` was called
            return self.data["timestamp"]
        return self.st_mtime

    def load(self, expect_tmp: bool = False) -> None:
        """Load data from disk.

        Prioritizes the final file (`job.json`) over the temporary file (`job.tmp`).
        Sets the `_mutable` flag accordingly.

        Parameters
        ----------
        expect_tmp : bool
            If True, raise RuntimeError if no temporary file is found when no final file exists.

        Notes
        -----
        This method updates internal mutability state:
        - `job.json` found -> immutable (`is_final == True`)
        - otherwise `job.tmp` found -> mutable

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

    def write(self, force: bool = False) -> None:
        """Write current data atomically.

        Parameters
        ----------
        force : bool, optional
            If True, allow writing even when finalized. In that case data is
            written directly to `job.json`; otherwise writes target `job.tmp`.

        """
        if not force and not self._mutable:
            raise RuntimeError("ExeData can't write after finalize!")
        if not self.is_valid(convert_to_type=True):
            raise ValueError("ExeData can't write data in an invalid state!")

        # Atomic write: write to a swap file first, then move into place.
        temp_swp = self.file_tmp.with_suffix(".swp")
        with open(temp_swp, "w") as tmp:
            json.dump(self.data, tmp, indent=2)
        shutil.move(temp_swp, self.file_tmp if self._mutable else self.file_fin)

    def finalize(self) -> None:
        """Finalize metadata by moving from `job.tmp` to `job.json`.

        This operation is idempotent; calling it on already-finalized data is
        a no-op.
        """
        if not self._mutable:
            # Idempotent: if already final, do nothing or check?
            # raise RuntimeError("ExeData already finalized?!")
            return

        self.write()
        shutil.move(self.file_tmp, self.file_fin)
        self._mutable = False

    def make_mutable(self) -> None:
        """Revert finalized state to mutable (`job.json` -> `job.tmp`)."""
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

    def scan_dir(
        self,
        skip_files: list[str] | None = None,
        force: bool = False,
        *,
        reset_output: bool = False,
        fs_max_retry: int = 1,
        fs_delay: float = 0.0,
    ) -> None:
        """Scan directory for output files and update job data.

        Parameters
        ----------
        skip_files : list[str], optional
            List of filenames to ignore.
        force : bool
            Force scan even if immutable. This updates in-memory data only;
            call `write(force=True)` to persist in finalized mode.
        **kwargs :
            reset_output (bool): Clear existing `output_files` list first.
            fs_max_retry (int): Number of scan retries before giving up.
            fs_delay (float): Delay between retries in seconds.

        Notes
        -----
        - New output files are filtered by `timestamp` (older files are ignored).
        - Parsed log payloads are merged into corresponding `jobs` entries.
        - Parsing failures are ignored to keep recovery best-effort.

        """
        if not self._mutable and not force:
            return

        skip_entries: list[str] = [ExeData._file_tmp, ExeData._file_fin, "job.run"]
        if self.data["mode"] == ExecutionMode.PRODUCTION:
            skip_entries.extend(self.data.get("input_files", []))
        if skip_files:
            skip_entries.extend(skip_files)

        self.data.setdefault("output_files", [])
        self.data.setdefault("lost_files", [])
        self.data.setdefault("jobs", {})

        if reset_output:
            self.data["output_files"] = []
            self.data["lost_files"] = []

        # > small buffer to account for fs timestamp resolution and potential clock skew
        # timestamp: float = self.data.setdefault("timestamp", 0.0) - 10.0

        old_output_files: set[str] = set(self.data["output_files"])
        found_new = False
        for _ in range(fs_max_retry):
            for entry in os.scandir(self.path):
                if not entry.is_file():
                    continue
                if entry.name in old_output_files:
                    old_output_files.remove(entry.name)
                if entry.name in skip_entries:
                    continue
                # > even with a buffer, this simply does not perform robustly
                # try:
                #     entry_mtime = entry.stat().st_mtime
                # except FileNotFoundError:
                #     # File was removed between scandir listing and stat call.
                #     continue
                # if timestamp > 0 and entry_mtime < timestamp:
                #     continue

                if entry.name not in self.data["output_files"]:
                    self.data["output_files"].append(entry.name)
                    found_new = True

            if found_new:
                break
            time.sleep(fs_delay)

        # > Some files disappeared? keep track
        if old_output_files:
            lost_files = set(self.data["lost_files"]) | old_output_files
            self.data["lost_files"] = list(lost_files)

        # > Parse per-seed logs and populate job entries.
        for job_data in self.data["jobs"].values():
            log_matches = [of for of in self.data["output_files"] if of.endswith(f".s{job_data['seed']}.log")]
            if len(log_matches) != 1:
                continue
            try:
                parsed_data = parse_log_file(Path(self.path) / log_matches[0])
                for key in parsed_data:
                    job_data[key] = parsed_data[key]
            except Exception:
                continue

    def remove_job(self, job_id: int, force: bool = False) -> None:
        """Remove one job entry and associated seed-tagged output files.

        Parameters
        ----------
        job_id : int
            Job id key in `data["jobs"]`.
        force : bool, optional
            Allow modification in finalized state (writes to `job.json`).

        Notes
        -----
        Removes files associated with the job seed (pattern `*.s<seed>.*` in
        tracked outputs). If an artifact is a symlink, its target is removed
        first, then the symlink itself.

        """
        if not force and not self._mutable:
            raise RuntimeError("ExeData can't modify after finalize!")
        jobs = self.data.get("jobs", {})
        output_files: list[str] = self.data.setdefault("output_files", [])

        def remove_artifact(path: Path) -> None:
            """Remove one artifact path, deleting symlink source before link."""
            if path.is_symlink():
                try:
                    source = path.resolve(strict=True)
                    if source.exists() and (source.is_file() or source.is_symlink()):
                        source.unlink()
                except FileNotFoundError:
                    # broken link; still remove link itself below
                    pass
                path.unlink(missing_ok=True)
                return
            if path.exists():
                path.unlink()

        job_key = job_id if job_id in jobs else str(job_id)
        if job_key in jobs:
            seed: int = jobs[job_key]["seed"]
            del_list: list[str] = [of for of in output_files if f".s{seed}." in of]
            for del_file in del_list:
                del_path = self.path / del_file
                remove_artifact(del_path)
                output_files.remove(del_file)
            del jobs[job_key]
        self.write(force)

    @property
    def is_complete(self) -> bool:
        """Return True if there is at least one job and all jobs have `result`."""
        if not self.data.get("jobs", {}):
            return False
        return all("result" in job_data for job_data in self.data.get("jobs", {}).values())
