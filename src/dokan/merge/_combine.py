"""standalone `nnlojet-merge` driver

Parses a `combine.ini` (the `nnlojet-combine.py` configuration format) and builds
a luigi DAG that drives the dokan merge core (`MergeObs`) without any database.
The three stages mirror `nnlojet-combine.py`:

- ``[Parts]``  per-Part statistical merge of seed `.dat` files (via `MergeObs`),
- ``[Merge]``  optional intermediate additive combinations,
- ``[Final]``  additive assembly of named parts into per-order results.

Only the additive ``+`` operator is supported; ``|`` and ``&`` raise
``NotImplementedError``.  Outputs land under ``out_dir/Parts`` and
``out_dir/Final`` to match `nnlojet-combine.py`.
"""

import ast
import configparser
import glob
import json
import os
import re
import sys
import time
from pathlib import Path

import luigi
import numpy as np

from ..task import Task
from ._core import (
    MergeObs,
    _accumulate_dat,
    _read_nx,
    _write_dat,
    _write_weights,
    build_obs_group,
)

# > NNLOJET seed-file pattern: <proc>.<...>.<...>.<obs>.s<seed>.dat -> group(2) is the observable
_OBS_FILE_RE = re.compile(r".*?/?([^./]+\.){3}([^/]+)\.s[0-9]+\.dat")


def _warn(msg: str) -> None:
    print(f"[nnlojet-merge] warning: {msg}", file=sys.stderr)


def _stamp(path: Path, ref_mtime: float) -> None:
    """Bump `path`'s mtime strictly past `ref_mtime` (and now) for monotonic freshness.

    Mirrors `MergeObs`, which forward-stamps its `.dat` output; downstream
    additive stages must do the same so their outputs never look stale against
    the freshly-written inputs they consumed.
    """
    t = max(time.time(), ref_mtime) + 1.0
    os.utime(path, (t, t))


def _load_default_merge() -> dict:
    """Load dokan's `config.json` `merge` defaults (base for `[Options]` overrides)."""
    cfg_path = Path(__file__).resolve().parents[1] / "config.json"
    try:
        with open(cfg_path) as f:
            return dict(json.load(f)["merge"])
    except (OSError, ValueError, KeyError):
        return {
            "trim_threshold": 8,
            "trim_max_fraction": 0.007,
            "k_scan_nsteps": 3,
            "k_scan_maxdev_steps": 0.4,
        }


def _apply_options(cp: configparser.ConfigParser, merge_cfg: dict) -> None:
    """Override `merge_cfg` from `[Options]`, reusing combine's `ast.literal_eval` grammar."""
    if cp.has_option("Options", "trim") and (raw := cp.get("Options", "trim")) is not None:
        val = ast.literal_eval(raw)
        if isinstance(val, bool):
            if not val:
                merge_cfg["trim_threshold"] = 0.0  # disable (MergeObs gates on > 0.0)
        elif isinstance(val, (int, float)):
            merge_cfg["trim_threshold"] = float(val)
        elif isinstance(val, (list, tuple)) and len(val) == 2:
            merge_cfg["trim_threshold"] = float(val[0])
            merge_cfg["trim_max_fraction"] = float(val[1])
        else:
            raise ValueError(f"combine: invalid 'trim' option: {val!r}")

    if cp.has_option("Options", "k-scan") and (raw := cp.get("Options", "k-scan")) is not None:
        val = ast.literal_eval(raw)
        if isinstance(val, bool):
            if not val:
                merge_cfg["k_scan_nsteps"] = 0  # disable (MergeObs gates on <= 0)
        elif isinstance(val, (int, float)):
            _warn("'k-scan' scalar (maxdev_unwgt) has no MergeObs equivalent; ignoring")
        elif isinstance(val, (list, tuple)) and len(val) == 2:
            merge_cfg["k_scan_nsteps"] = int(val[0])
            merge_cfg["k_scan_maxdev_steps"] = float(val[1])
        elif isinstance(val, (list, tuple)) and len(val) == 3:
            _warn("'k-scan' maxdev_unwgt (first element) has no MergeObs equivalent; ignoring it")
            merge_cfg["k_scan_nsteps"] = int(val[1])
            merge_cfg["k_scan_maxdev_steps"] = float(val[2])
        else:
            raise ValueError(f"combine: invalid 'k-scan' option: {val!r}")

    if cp.has_option("Options", "weighted"):
        _warn("'weighted' option has no MergeObs equivalent (always weighted k-scan); ignoring")
    if cp.has_option("Options", "columns"):
        _warn("'columns' option is not supported in nnlojet-merge v1; ignoring")
    if cp.has_option("Options", "plot"):
        _warn("'plot' option is not supported in nnlojet-merge; ignoring")


def _parse_operands(spec: str) -> list[str]:
    """Parse a `[Merge]`/`[Final]` right-hand side; only the additive `+` operator is supported."""
    spec = spec.strip()
    if "|" in spec or "&" in spec:
        raise NotImplementedError(
            f"combine: the '|' and '&' merge operators are not supported yet (got: {spec!r})"
        )
    return [p.strip() for p in spec.split("+")]


def _discover_observables(
    obs_options: list[str], raw_dir: Path, part_dirs: list[str], recursive: bool
) -> dict[str, dict]:
    """Resolve the observable *name* set from `[Observables]`.

    `ALL` triggers a filesystem scan of every Part directory (filenames only — no
    file contents are read); any other entries are taken as explicit observable
    names.  Each observable's `nx` is deliberately **not** resolved here: it is
    read from the `#nx` marker of the data files during staging (`build_obs_group`)
    and the additive stages (`_CombineSum`), so the file is the single source of
    truth.  Returns the `histograms` mapping (per-observable metadata is filled in
    downstream from the files).
    """
    discover_all = "ALL" in obs_options
    names: list[str] = [obs for obs in obs_options if obs != "ALL"]

    if discover_all:
        for part_dir in part_dirs:
            base = raw_dir / part_dir
            pattern = str(base / "**" / "*.dat") if recursive else str(base / "*.dat")
            for f in glob.glob(pattern, recursive=recursive):
                m = _OBS_FILE_RE.search(f)
                if not m:
                    _warn(f"could not extract observable name from file: {f}")
                    continue
                obs = m.group(2)
                if obs not in names:
                    names.append(obs)

    return {obs: {} for obs in names}


def build_config(ini_path: str | os.PathLike) -> dict:
    """Parse a `combine.ini` into the synthesized `config` dict shared by all tasks."""
    cp = configparser.ConfigParser(
        allow_no_value=True,
        delimiters=("=", ":"),
        comment_prefixes=("#",),
        inline_comment_prefixes=("#",),
        empty_lines_in_values=False,
    )
    cp.optionxform = lambda option: option  # type: ignore[assignment]  # preserve case
    if not cp.read(ini_path):
        raise FileNotFoundError(f"combine: configuration file not found: {ini_path}")

    raw_dir = Path(cp.get("Paths", "raw_dir")).resolve()
    out_dir = Path(cp.get("Paths", "out_dir")).resolve()
    recursive = cp.getboolean("Options", "recursive", fallback=False)
    weights = cp.getboolean("Options", "weights", fallback=False)

    merge_cfg = _load_default_merge()
    _apply_options(cp, merge_cfg)

    # > [Parts]: option is the raw sub-directory, value an optional output alias
    parts: dict[str, str] = {}
    if cp.has_section("Parts"):
        for part_dir in cp.options("Parts"):
            alias = cp.get("Parts", part_dir)
            parts[part_dir] = alias if alias is not None else part_dir

    obs_options = cp.options("Observables") if cp.has_section("Observables") else []
    histograms = _discover_observables(obs_options, raw_dir, list(parts.keys()), recursive)

    merge: dict[str, list[str]] = {}
    if cp.has_section("Merge"):
        for name in cp.options("Merge"):
            merge[name] = _parse_operands(cp.get("Merge", name))

    final: dict[str, list[str]] = {}
    if cp.has_section("Final"):
        for name in cp.options("Final"):
            final[name] = _parse_operands(cp.get("Final", name))

    return {
        "run": {"path": str(out_dir), "histograms": histograms},
        "merge": merge_cfg,
        "combine": {
            "raw_dir": str(raw_dir),
            "recursive": recursive,
            "weights": weights,
            "parts": parts,
            "merge": merge,
            "final": final,
        },
    }


def _producer(config: dict, name: str) -> Task:
    """Map an operand name to the task that writes `Parts/<name>.<obs>.dat`."""
    combine = config["combine"]
    for part_dir, alias in combine["parts"].items():
        if alias == name:
            return CombinePart(config=config, part_dir=part_dir, alias=alias)
    if name in combine["merge"]:
        return CombineMerge(config=config, name=name)
    raise ValueError(f"combine: unknown operand '{name}' (not a Part alias or a Merge entry)")


class CombinePart(Task):
    """Per-Part merge: glob seed `.dat` files, stage to HDF5, dispatch `MergeObs` per observable."""

    part_dir: str = luigi.Parameter()  # type: ignore[assignment]
    alias: str = luigi.Parameter()  # type: ignore[assignment]

    priority = 130

    def _glob_inputs(self) -> dict[str, list[str]]:
        combine = self.config["combine"]
        raw_dir = Path(combine["raw_dir"])
        recursive = combine["recursive"]
        inputs: dict[str, list[str]] = {}
        for obs in self.config["run"]["histograms"]:
            base = raw_dir / self.part_dir
            if recursive:
                files = glob.glob(str(base / "**" / f"*.{obs}.s[0-9]*.dat"), recursive=True)
            else:
                files = glob.glob(str(base / f"*.{obs}.s[0-9]*.dat"))
            if files:
                # > absolute paths: `base_path / abs` resolves to `abs`, so the staging
                # > HDF5 and the weights output reference the real seed files directly.
                inputs[obs] = sorted(str(Path(f).resolve()) for f in files)
        return inputs

    def complete(self) -> bool:
        inputs = self._glob_inputs()
        if not inputs:
            return True  # nothing to merge for this Part
        parts_dir = self._path / "Parts"
        for obs, files in inputs.items():
            dat = parts_dir / f"{self.alias}.{obs}.dat"
            if not dat.is_file():
                return False
            if dat.stat().st_mtime < max(Path(f).stat().st_mtime for f in files):
                return False
        return True

    def run(self):  # type: ignore[override]
        inputs = self._glob_inputs()
        if not inputs:
            return
        parts_dir = self._path / "Parts"
        parts_dir.mkdir(parents=True, exist_ok=True)
        hdf5_dir = self._path / ".hdf5"
        hdf5_dir.mkdir(parents=True, exist_ok=True)
        hdf5_file = hdf5_dir / f"{self.part_dir}.hdf5"

        histograms = self.config["run"]["histograms"]
        try:
            build_obs_group(hdf5_file, str(self.part_dir), inputs, histograms, self._path)
        except ValueError as e:
            if not str(e).startswith("stale HDF5 cache:"):
                raise
            # > the HDF5 file is a disposable cache; an incompatible one (e.g. written
            # > before nx was sourced from the file headers) is rebuilt from scratch
            _warn(f"rebuilding stale HDF5 cache {hdf5_file.name}: {e}")
            hdf5_file.unlink(missing_ok=True)
            build_obs_group(hdf5_file, str(self.part_dir), inputs, histograms, self._path)

        weights = self.config["combine"]["weights"]
        pending: list[MergeObs] = []
        for obs in inputs:
            dat_out = (parts_dir / f"{self.alias}.{obs}.dat").relative_to(self._path)
            wgt_out = (
                str((parts_dir / f"{self.alias}.{obs}.weights.txt").relative_to(self._path))
                if weights
                else None
            )
            mrg = MergeObs(
                config=self.config,
                hdf5_in=str(hdf5_file.relative_to(self._path)),
                hdf5_path=[self.part_dir, obs],
                dat_out=str(dat_out),
                wgt_out=wgt_out,
                grids=False,
            )
            if not mrg.complete():
                pending.append(mrg)
        if pending:
            yield pending


class _CombineSum(Task):
    """Additive combination of `Parts/<operand>.<obs>.dat` files (shared by Merge & Final)."""

    name: str = luigi.Parameter()  # type: ignore[assignment]

    _section: str = ""  # "merge" or "final"
    _out_subdir: str = ""  # "Parts" or "Final"

    def _operands(self) -> list[str]:
        return list(self.config["combine"][self._section][self.name])

    def requires(self):
        return [_producer(self.config, op) for op in self._operands()]

    def _in_files(self, obs: str) -> list[str]:
        files = []
        for op in self._operands():
            ptfile = self._path / "Parts" / f"{op}.{obs}.dat"
            if ptfile.is_file():
                files.append(str(ptfile.relative_to(self._path)))
        return files

    def complete(self) -> bool:
        # > wait for all operand producers before judging freshness (their outputs feed us)
        if any(not req.complete() for req in self.requires()):
            return False
        out_dir = self._path / self._out_subdir
        for obs in self.config["run"]["histograms"]:
            in_files = self._in_files(obs)
            if not in_files:
                continue  # no operand data for this observable
            out_file = out_dir / f"{self.name}.{obs}.dat"
            if not out_file.is_file():
                return False
            newest = max((self._path / f).stat().st_mtime for f in in_files)
            if out_file.stat().st_mtime < newest:
                return False
        return True

    def run(self):  # type: ignore[override]
        out_dir = self._path / self._out_subdir
        out_dir.mkdir(parents=True, exist_ok=True)
        weights = self.config["combine"]["weights"]
        for obs in self.config["run"]["histograms"]:
            in_files = self._in_files(obs)
            if not in_files:
                continue
            # > nx is read from the operand `.dat` headers (single source of truth)
            nx = _read_nx(self._path / in_files[0])
            if nx is None:
                _warn(f"{self.name}.{obs}: could not determine #nx from {in_files[0]}; skipping")
                continue
            acc = _accumulate_dat(in_files, nx, self._path, on_error=lambda f, e: _warn(f"{f}: {e!r}"))
            if acc is None:
                _warn(f"{self.name}.{obs}: no usable input files")
                continue
            labels, neval, xval, hist, used = acc
            out_file = out_dir / f"{self.name}.{obs}.dat"
            _write_dat(out_file, labels, neval, nx, xval, hist)
            newest = max((self._path / u).stat().st_mtime for u in used)
            _stamp(out_file, newest)
            if weights:
                wgt_file = out_file.with_suffix(".weights.txt")
                filenames = [str((self._path / u).absolute()) for u in used]
                ones = np.ones((hist.shape[0], len(filenames)), dtype=np.float64)
                _write_weights(wgt_file, nx, xval, filenames, ones)


class CombineMerge(_CombineSum):
    """`[Merge]` intermediate sum, written into `Parts/` so `[Final]` can reference it."""

    _section = "merge"
    _out_subdir = "Parts"
    priority = 120


class CombineFinal(_CombineSum):
    """`[Final]` per-order assembly, written into `Final/`."""

    _section = "final"
    _out_subdir = "Final"
    priority = 110


class Combine(Task):
    """Root wrapper: pulls every Part, Merge and Final task into one DAG."""

    priority = 100

    def requires(self):
        combine = self.config["combine"]
        reqs: list[Task] = [
            CombinePart(config=self.config, part_dir=part_dir, alias=alias)
            for part_dir, alias in combine["parts"].items()
        ]
        reqs += [CombineMerge(config=self.config, name=name) for name in combine["merge"]]
        reqs += [CombineFinal(config=self.config, name=name) for name in combine["final"]]
        return reqs

    def complete(self) -> bool:
        return all(req.complete() for req in self.requires())

    def run(self):  # type: ignore[override]
        return None
