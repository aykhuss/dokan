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
import fnmatch
import glob
import json
import logging
import os
import re
import sys
import time
from pathlib import Path

import h5py
import luigi
import numpy as np

from ..task import Task
from ..util import file_fingerprint, read_json_sidecar, write_json_sidecar
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


_log = logging.getLogger(__name__)


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

    def _hdf5_file(self) -> Path:
        return self._path / ".hdf5" / f"{self.part_dir}.hdf5"

    def _make_merge_obs(self, obs: str, hdf5_file: Path) -> MergeObs:
        """Construct the `MergeObs` for one observable (shared by run() and complete())."""
        parts_dir = self._path / "Parts"
        wgt_out = (
            str((parts_dir / f"{self.alias}.{obs}.weights.txt").relative_to(self._path))
            if self.config["combine"]["weights"]
            else None
        )
        return MergeObs(
            config=self.config,
            hdf5_in=str(hdf5_file.relative_to(self._path)),
            hdf5_path=[self.part_dir, obs],
            dat_out=str((parts_dir / f"{self.alias}.{obs}.dat").relative_to(self._path)),
            wgt_out=wgt_out,
            grids=False,
        )

    def _seed_fingerprint_file(self) -> Path:
        return self._hdf5_file().with_suffix(".seeds.json")

    def _seed_fingerprint(self, inputs: dict[str, list[str]]) -> dict:
        return {obs: file_fingerprint(files) for obs, files in inputs.items()}

    def _read_seed_fingerprints(self) -> dict | None:
        return read_json_sidecar(self._seed_fingerprint_file())

    def _write_seed_fingerprints(self, inputs: dict[str, list[str]]) -> None:
        write_json_sidecar(self._seed_fingerprint_file(), self._seed_fingerprint(inputs))

    def _stale_part_outputs(self) -> list[Path]:
        """Existing `Parts` outputs for observables this part has no seeds for any more.

        Keyed on actual raw-seed existence (not the configured observable set), so an observable
        whose seeds have all disappeared is cleaned up — while one the user merely narrowed out of
        `[Observables]` this run, but whose seeds still exist, is preserved.  `_CombineSum` would
        otherwise keep consuming the orphaned `.dat`.
        """
        parts_dir = self._path / "Parts"
        if not parts_dir.is_dir():
            return []
        combine = self.config["combine"]
        raw_base = Path(combine["raw_dir"]) / self.part_dir
        recursive = combine["recursive"]
        # > one disk traversal of the seed tree; observable membership is matched in memory
        pattern = "**/*.s[0-9]*.dat" if recursive else "*.s[0-9]*.dat"
        seeds = [Path(s).name for s in glob.glob(str(raw_base / pattern), recursive=recursive)]
        prefix = f"{self.alias}."
        stale: list[Path] = []
        for dat in parts_dir.glob(f"{self.alias}.*.dat"):
            obs = dat.name[len(prefix) : -len(".dat")]
            if not any(fnmatch.fnmatch(s, f"*.{obs}.s[0-9]*.dat") for s in seeds):
                stale.append(dat)
        return stale

    def _cache_is_current(self, hdf5_file: Path, inputs: dict[str, list[str]]) -> bool:
        """Whether the staging HDF5 cache was built from exactly the current raw seeds.

        Compares a stored `(path, size, mtime_ns)` fingerprint of the seeds ingested at build
        time against the current seeds — an exact identity match (not mtime ordering), so
        additions, removals and in-place overwrites all invalidate the cache and it is immune to
        clock skew.  `build_obs_group` is append-only by path, so `run()` rebuilds on mismatch.
        """
        if not hdf5_file.is_file():
            return False
        if self._read_seed_fingerprints() != self._seed_fingerprint(inputs):
            return False
        # > sanity: the cache actually holds each observable's data (guards manual corruption)
        with h5py.File(hdf5_file, "r", libver="latest", swmr=True) as h5f:
            grp_pt = h5f.get(self.part_dir)
            if grp_pt is None:
                return False
            for obs in inputs:
                obs_grp = grp_pt.get(obs)
                if obs_grp is None or "data" not in obs_grp:
                    return False
        return True

    def complete(self) -> bool:
        inputs = self._glob_inputs()
        if self._stale_part_outputs():
            return False  # leftover outputs for vanished observables to clean up
        if not inputs:
            return True  # nothing to merge for this Part
        hdf5_file = self._hdf5_file()
        # > the cache must faithfully reflect the current raw seeds before MergeObs.complete() —
        # > the freshness authority, which only sees the cache, not the seeds — can be trusted
        if not self._cache_is_current(hdf5_file, inputs):
            return False
        return all(self._make_merge_obs(obs, hdf5_file).complete() for obs in inputs)

    def run(self):  # type: ignore[override]
        inputs = self._glob_inputs()
        # > drop outputs for observables that no longer have any seeds, so stale per-Part data is
        # > never summed downstream (build_obs_group is append-only and cannot do this itself)
        for stale in self._stale_part_outputs():
            _log.info("Part %s: removing stale output %s", self.alias, stale.name)
            stale.unlink(missing_ok=True)
            stale.with_suffix(".weights.txt").unlink(missing_ok=True)
            stale.with_suffix(".merged.json").unlink(missing_ok=True)
        if not inputs:
            return
        parts_dir = self._path / "Parts"
        parts_dir.mkdir(parents=True, exist_ok=True)
        hdf5_dir = self._path / ".hdf5"
        hdf5_dir.mkdir(parents=True, exist_ok=True)
        hdf5_file = self._hdf5_file()

        # > build_obs_group is append-only by path; if the cache no longer matches the seeds
        # > (a seed was added, removed or overwritten in place) rebuild it from scratch
        if hdf5_file.is_file() and not self._cache_is_current(hdf5_file, inputs):
            _log.info("Part %s: raw seeds changed; rebuilding HDF5 cache", self.alias)
            # > drop the per-observable merge outputs whose seeds changed: MergeObs' own freshness
            # > token is the max seed mtime, which misses an in-place overwrite with a lower mtime,
            # > so deleting the `.dat` forces it to re-merge from the rebuilt cache
            stored_fp = self._read_seed_fingerprints() or {}
            current_fp = self._seed_fingerprint(inputs)
            for obs in inputs:
                if stored_fp.get(obs) != current_fp.get(obs):
                    for suffix in (".dat", ".weights.txt", ".merged.json"):
                        (parts_dir / f"{self.alias}.{obs}{suffix}").unlink(missing_ok=True)
            hdf5_file.unlink()

        n_files = sum(len(v) for v in inputs.values())
        _log.info("Part %s: staging %d seed file(s) for %d observable(s)", self.alias, n_files, len(inputs))
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
        # > record the seeds this cache was built from, for the next `_cache_is_current()` check
        self._write_seed_fingerprints(inputs)

        pending: list[MergeObs] = []
        for obs in inputs:
            mrg = self._make_merge_obs(obs, hdf5_file)
            if not mrg.complete():
                pending.append(mrg)
        if pending:
            for mrg in pending:
                _log.info("Part %s: merging %s", self.alias, mrg.hdf5_path[-1])
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

    def _operand_fingerprint(self, in_files: list[str]) -> list:
        return file_fingerprint([str(self._path / f) for f in in_files])

    def _fingerprint_file(self) -> Path:
        return self._path / self._out_subdir / f"{self.name}.operands.json"

    def _read_fingerprints(self) -> dict:
        fp = read_json_sidecar(self._fingerprint_file())
        return fp if isinstance(fp, dict) else {}

    def _write_fingerprints(self, fp: dict) -> None:
        write_json_sidecar(self._fingerprint_file(), fp)

    def _stale_outputs(self) -> list[Path]:
        """Existing outputs for observables that now have no operand inputs on disk.

        Keyed on actual operand existence (disk), so it also catches an observable that has
        dropped out of the (re-discovered) configuration entirely, not just configured ones.
        """
        out_dir = self._path / self._out_subdir
        if not out_dir.is_dir():
            return []
        prefix = f"{self.name}."
        stale: list[Path] = []
        for out_file in out_dir.glob(f"{self.name}.*.dat"):
            obs = out_file.name[len(prefix) : -len(".dat")]
            if not self._in_files(obs):
                stale.append(out_file)
        return stale

    def complete(self) -> bool:
        # > wait for all operand producers before judging freshness (their outputs feed us)
        if any(not req.complete() for req in self.requires()):
            return False
        if self._stale_outputs():
            return False  # leftover outputs for observables that lost all operands
        weights = self.config["combine"]["weights"]
        out_dir = self._path / self._out_subdir
        stored_fp = self._read_fingerprints()
        for obs in self.config["run"]["histograms"]:
            in_files = self._in_files(obs)
            if not in_files:
                continue  # no operand data for this observable
            out_file = out_dir / f"{self.name}.{obs}.dat"
            if not out_file.is_file():
                return False
            entry = stored_fp.get(obs)
            if not isinstance(entry, dict):
                return False  # missing/legacy sidecar -> re-sum
            # > identity check: the output must have been summed from exactly the current operand
            # > set/content (catches operands added, removed, or re-produced) — not mtime ordering
            if entry.get("operands") != self._operand_fingerprint(in_files):
                return False
            # > a requested weights table is a required artifact and must have been co-produced with
            # > this output (recorded in the sidecar, so a stale leftover can't satisfy a re-enable)
            if weights and (not entry.get("weights") or not out_file.with_suffix(".weights.txt").is_file()):
                return False
        return True

    def run(self):  # type: ignore[override]
        out_dir = self._path / self._out_subdir
        out_dir.mkdir(parents=True, exist_ok=True)
        # > drop outputs for observables that no longer have any operands (else they linger as
        # > stale results and could be consumed by a further [Final] stage)
        for stale in self._stale_outputs():
            _log.info("%s: removing stale output %s", self.name, stale.name)
            stale.unlink(missing_ok=True)
            stale.with_suffix(".weights.txt").unlink(missing_ok=True)
        weights = self.config["combine"]["weights"]
        fp: dict = {}
        for obs in self.config["run"]["histograms"]:
            in_files = self._in_files(obs)
            if not in_files:
                continue
            _log.info("%s [%s]: combining %d file(s)", self.name, obs, len(in_files))
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
            # > record the operand identity and whether weights were produced (see `complete()`)
            fp[obs] = {"operands": self._operand_fingerprint(in_files), "weights": weights}
        self._write_fingerprints(fp)


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
