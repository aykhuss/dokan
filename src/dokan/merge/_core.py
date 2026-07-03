"""dokan merge core

DB-free statistical merge of NNLOJET histogram results: the `MergeObs` luigi
task plus the pure `.dat`/HDF5 helpers it builds on.  This module holds the
single source of truth for the merge algorithm (double-MAD outlier trimming,
pairwise merging and k-scan, weighted averaging) and the `.dat` file format,
shared by the dokan workflow (`dokan.db._dbmerge`) and the standalone
`nnlojet-merge` tool.  Nothing here touches the database.
"""

import os
import re
import subprocess
import time
from enum import IntEnum, unique
from pathlib import Path

import h5py
import luigi
import numpy as np

from .._types import GenericPath
from ..task import Task
from ..util import is_finite_number, read_json_sidecar, write_json_sidecar


@unique
class BinMask(IntEnum):
    """possible values for the bin mask"""

    ACTIVE = 0
    TRIMMED = 1
    INVALID = 2


_comment_prefix = "#"
_NX_RE = re.compile(
    rf"^\s*{re.escape(_comment_prefix)}\s*nx\s*(?::|=)?\s*(\d+)\s*$",
    re.IGNORECASE,
)

# > some variable definitions
_dt_vstr = h5py.string_dtype()
_dt_hist = np.dtype([("result", np.float64), ("error2", np.float64)])  # HDF5 storage: per-job, per-bin
_dt_cmlt = np.dtype([("neval", np.int64), ("sumf", np.float64), ("sumf2", np.float64)])  # in-memory cumulants
_chunk_size: int = 256  # chunk size along the ndat axis
_MAD_NORMAL_SCALE: float = 0.6745  # median absolute deviation to 1-sigma for a normal distribution


def _obs_has_grid(hist_info: dict) -> bool:
    """Return whether this observable has an associated PineAPPL grid."""
    return hist_info.get("grid") is not None


def _write_dat(
    path: Path,
    labels: str | None,
    neval: int,
    nx: int,
    xval: np.ndarray | None,
    hist: np.ndarray,
) -> None:
    """Write a merged histogram to a `.dat` file.

    `hist` is an `(nrows, ncols)` array of `_dt_hist` records (its `error2` field
    holds the standard error, not the variance). `xval` is `(nrows, nx)` or `None`
    when `nx == 0`; overflow rows are flagged by all-NaN `xval` entries. The number
    format is the single source of truth shared with the per-`Part` merge. A
    trailing `#nx:` line records the x-column count, matching the NNLOJET input
    files (see `_read_dat`).

    The file is written atomically (tmp sibling + rename): merge tasks can run
    concurrently with readers (e.g. `MergeAll` accumulating part `.dat` files),
    and an in-place rewrite would expose torn/partial files to them.
    """
    nrows, ncols = hist.shape
    tmp_path = path.with_name(path.name + ".tmp")
    with open(tmp_path, "w") as df:
        if labels is not None:
            df.write(labels + "\n")
        df.write(f"#neval: {neval}\n")
        for irow in range(nrows):
            if xval is not None:
                if np.all(np.isnan(xval[irow])):
                    if nx == 3:
                        df.write("#overflow:lower center upper ")
                    else:
                        df.write("#overflow: ")
                else:
                    for x in xval[irow]:
                        df.write(f"{np.format_float_scientific(x): <25} ")
            for icol in range(ncols):
                df.write(f"{np.format_float_scientific(hist['result'][irow, icol]): <25} ")
                df.write(f"{np.format_float_scientific(hist['error2'][irow, icol]): <25} ")
            df.write("\n")
        df.write(f"#nx: {nx}\n")
    os.replace(tmp_path, path)


def _write_weights(
    path: Path,
    nx: int,
    xval: np.ndarray | None,
    filenames: list[str],
    weights: np.ndarray,
) -> None:
    """Write the interpolation-grid weights file.

    `weights` is an `(nrows, ndat)` array; row `i`, column `j` is the weight of
    input `filenames[j]` in bin `i`. Overflow rows (all-NaN `xval`) are skipped.

    Written atomically (tmp sibling + rename) for the same reason as `_write_dat`.
    """
    nrows = weights.shape[0]
    ndat = len(filenames)
    tmp_path = path.with_name(path.name + ".tmp")
    with open(tmp_path, "w") as wf:
        wf.write(f"#nx={nx} ")
        if xval is not None and nx == 3:
            for irow in range(nrows):
                if np.all(np.isnan(xval[irow])):
                    continue
                wf.write(
                    f"[{np.format_float_scientific(xval[irow][0])},"
                    f"{np.format_float_scientific(xval[irow][-1])}] "
                )
        wf.write("\n")
        for idat in range(ndat):
            wf.write(filenames[idat] + " ")
            for irow in range(nrows):
                if xval is not None and np.all(np.isnan(xval[irow])):
                    continue
                wf.write(np.format_float_scientific(weights[irow, idat]) + " ")
            wf.write("\n")
    os.replace(tmp_path, path)


def _parse_nx_marker(line: str) -> int | None:
    """Parse a `#nx` marker line, returning `None` for non-`#nx` comments."""
    if match := _NX_RE.fullmatch(line):
        return int(match.group(1))

    body = line.strip()
    if body.startswith(_comment_prefix):
        body = body[len(_comment_prefix) :].lstrip().lower()
        if body.startswith("nx") and (len(body) == 2 or body[2] in " \t:="):
            raise ValueError(f"malformed #nx marker: {line!r}")
    return None


def _read_nx(path: Path) -> int | None:
    """Read the trailing `#nx` marker from an NNLOJET/dokan `.dat` file.

    `nx` is the number of x-columns (bin edges) and is the authoritative property
    of an observable's binning.  The marker is written as the final line (see
    `_write_dat`, matching the NNLOJET seed files), so only the file tail is read
    to avoid pulling large histograms into memory.  Returns `None` if no `#nx`
    marker is found (or the file cannot be read). Raises `ValueError` when a
    malformed `#nx` marker is present.
    """
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - 4096))
            tail = f.read().decode("utf-8", "replace")
    except OSError:
        return None
    for line in reversed(tail.splitlines()):
        nx = _parse_nx_marker(line)
        if nx is not None:
            return nx
    return None


def _read_dat(path: Path, nx: int | None = None) -> tuple[str | None, int, np.ndarray | None, np.ndarray]:
    """Read a merged `.dat` file written by `_write_dat`.

    Returns `(labels, neval, xval, hist)` where `hist` is an `(nrows, ncols)` array
    of `_dt_hist` records and `xval` is `(nrows, nx)` (NaN for overflow rows) or
    `None` when `nx == 0`. When `nx` is `None` it is taken from the file's `#nx:`
    line (the single source of truth); when supplied it is validated against that
    line. Raises `ValueError` on a malformed file or an `#nx:` mismatch.
    """
    labels: str | None = None
    neval: int | None = None
    file_nx: int | None = None
    rows: list[tuple[bool, list[str]]] = []  # (is_overflow, tokens)
    with open(path) as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            if line.startswith(_comment_prefix):
                body = line[len(_comment_prefix) :].lstrip().lower()
                parsed_nx = _parse_nx_marker(line)
                if parsed_nx is not None:
                    file_nx = parsed_nx
                elif body.startswith("overflow"):
                    rows.append((True, line.split()))
                elif body.startswith("labels"):
                    labels = line
                elif body.startswith("neval"):
                    neval = int(line.split()[1])
                # > any other comment line is ignored
                continue
            rows.append((False, line.split()))
    if neval is None:
        raise ValueError(f"missing #neval in {path}")
    # > resolve nx: trust the file when the caller leaves it unspecified, otherwise
    # > validate the caller's expectation against the file's own marker
    if nx is None:
        if file_nx is None:
            raise ValueError(f"missing #nx in {path}")
        nx = file_nx
    elif file_nx is not None and file_nx != nx:
        raise ValueError(f"nx mismatch in {path}: {file_nx} != {nx}")
    # > determine the number of (val, err) column pairs from a non-overflow row;
    # > overflow rows have a variable-length marker, so we read data from the tail
    ncols: int | None = None
    for is_overflow, tokens in rows:
        if not is_overflow:
            ndata = len(tokens) - nx
            if ndata <= 0 or ndata % 2 != 0:
                raise ValueError(f"malformed data row in {path}")
            ncols = ndata // 2
            break
    if ncols is None:
        raise ValueError(f"no data rows in {path}")
    nrows = len(rows)
    hist = np.empty((nrows, ncols), dtype=_dt_hist)
    xval = np.empty((nrows, nx), dtype=np.float64) if nx > 0 else None
    for irow, (is_overflow, tokens) in enumerate(rows):
        ydata = tokens[-2 * ncols :]
        if len(ydata) != 2 * ncols:
            raise ValueError(f"column count mismatch in {path}")
        hist["result"][irow] = [float(ydata[2 * i]) for i in range(ncols)]
        hist["error2"][irow] = [float(ydata[2 * i + 1]) for i in range(ncols)]
        if xval is not None:
            if is_overflow:
                xval[irow] = np.nan
            else:
                xval[irow] = [float(t) for t in tokens[:nx]]
    return labels, neval, xval, hist


def _accumulate_dat(
    files: list[str],
    nx: int | None = None,
    base_path: Path = Path(),
    on_error=None,
) -> tuple[str | None, int, np.ndarray | None, np.ndarray, list[str]] | None:
    """Sum already-merged per-`Part` `.dat` files for one observable.

    Files are resolved relative to `base_path`. `nx` may be left `None`, in which
    case it is read from each file's `#nx:` marker (binning consistency is then
    enforced via the per-file shape check). Accumulation is purely additive:
    results are summed bin-by-bin, errors in quadrature, and `neval` summed. A file
    that cannot be read, or whose binning is inconsistent with the running total
    (labels, x-values/overflow position, or bin count), is skipped; if `on_error`
    is given it is called as `on_error(file, exception)`.

    Returns `(labels, neval, xval, hist, used_files)` or `None` if no file could be
    accumulated. `hist['error2']` holds the combined standard error.
    """
    labels: str | None = None
    neval: int = 0
    xval: np.ndarray | None = None
    hist: np.ndarray | None = None
    used: list[str] = []
    for in_file in files:
        try:
            f_labels, f_neval, f_xval, f_hist = _read_dat(base_path / in_file, nx)
            if hist is None:
                labels, xval = f_labels, f_xval
                hist = f_hist.copy()
                # > start the running sum-of-squares for quadrature error combination
                np.square(hist["error2"], out=hist["error2"])
                neval = f_neval
            else:
                if f_hist.shape != hist.shape:
                    raise ValueError(f"shape mismatch: {f_hist.shape} != {hist.shape}")
                if f_labels != labels:
                    raise ValueError("labels mismatch")
                if (xval is None) != (f_xval is None):
                    raise ValueError("xval mismatch")
                if (
                    xval is not None
                    and f_xval is not None
                    and not np.array_equal(xval, f_xval, equal_nan=True)
                ):
                    raise ValueError("xval mismatch")
                hist["result"] += f_hist["result"]
                hist["error2"] += np.square(f_hist["error2"])
                neval += f_neval
            used.append(in_file)
        except (ValueError, OSError) as e:
            if on_error is not None:
                on_error(in_file, e)
    if hist is None:
        return None
    np.sqrt(hist["error2"], out=hist["error2"])
    return labels, neval, xval, hist, used


def _run_pineappl_merge(pine_merge: Path, wgt_file: Path, grid_file: Path, check: bool = True) -> int:
    """Combine per-input PineAPPL grids into `grid_file` using `nnlojet-merge-pineappl`.

    `wgt_file` lists each input file and its per-bin weight. Returns the subprocess
    return code; with `check=True` a non-zero exit raises. The caller is responsible
    for verifying `pine_merge` exists and is executable.
    """
    grid_log = grid_file.with_suffix(".log")
    cwd = grid_file.parent
    with open(grid_log, "w") as log:
        result = subprocess.run(
            [
                str(pine_merge),
                str(wgt_file.relative_to(cwd)),
                str(grid_file.relative_to(cwd)),
                "-v",
                "--skip",
                "--noopt",
            ],
            env=os.environ.copy(),
            cwd=cwd,
            stdout=log,
            stderr=log,
            text=True,
        )
    if check and result.returncode != 0:
        raise RuntimeError(f"nnlojet-merge-pineappl failed for {grid_file.name}. Check {grid_log}")
    return result.returncode


def build_obs_group(
    hdf5_file: Path,
    group_name: str,
    in_files: dict[str, list[GenericPath]],
    histograms: dict,
    base_path: Path,
    single_file: str | None = None,
    merge_in_progress: bool = False,
) -> dict[str, int]:
    """Ingest per-observable `.dat` files into an HDF5 group (DB-free).

    Extracted from `MergePart.run` so the dokan workflow and the standalone
    `nnlojet-merge` tool share one ingestion routine.  `histograms` maps each
    observable name to its metadata (`nx`, optional `cumulant`/`grid`); the
    paths in `in_files` are resolved relative to `base_path`.  Only files not
    already stored are appended (idempotent across calls); the per-observable
    group is created on first sight.  `group_name` is the top-level group (a
    `Part` name in the workflow).  Returns `{obs: ndat_total}` for every
    observable that received new data this call.
    """
    resize_obs: dict[str, int] = {}
    with h5py.File(hdf5_file, "a", libver="latest") as h5f:
        # > "single writer multiple reader" mode on for parallel reads
        h5f.swmr_mode = True

        # > retrieve top-level group; init group structure & data if needed
        h5grp_pt: h5py.Group = h5f.require_group(group_name)

        # > make sure all observables groups are in place with the correct attributes.
        # > `nx` is sourced from the input file's `#nx` marker when the caller leaves
        # > it unspecified (the standalone tool); the DB workflow passes the runcard
        # > `nx` (which also carries `cumulant`/`grid` not present in the `.dat` files).
        for obs, hist in histograms.items():
            hist_nx = hist.get("nx")
            nx: int | None = int(hist_nx) if hist_nx is not None else None
            if nx is None:
                files = in_files.get(obs)
                if not files:
                    continue  # no data to stage yet and no nx given: nothing to set up
                file_nx = _read_nx(base_path / files[0])
                if file_nx is None:
                    raise ValueError(f"could not determine #nx for observable '{obs}' from {files[0]}")
                nx = file_nx
            h5grp_obs: h5py.Group = h5grp_pt.require_group(f"{obs}")
            if "nx" in h5grp_obs.attrs and int(h5grp_obs.attrs["nx"]) != nx:
                # > pre-existing group with a different binning ⇒ stale cache; fail loudly
                # > rather than silently discard staged data (the caller owns rebuilding)
                raise ValueError(
                    f"stale HDF5 cache: observable '{obs}' in {Path(hdf5_file).name}: "
                    f"stored nx={int(h5grp_obs.attrs['nx'])} != input nx={nx}"
                )
            if "timestamp" not in h5grp_obs.attrs:
                h5grp_obs.attrs.create("timestamp", 0, dtype=np.float64)
            if "nx" not in h5grp_obs.attrs:
                h5grp_obs.attrs.create("nx", nx, dtype=np.int32)
            if "cumulant" in hist and "cumulant" not in h5grp_obs.attrs:
                h5grp_obs.attrs.create("cumulant", hist["cumulant"], dtype=np.int32)
            if "grid" in hist and "grid" not in h5grp_obs.attrs:
                h5grp_obs.attrs.create("grid", hist["grid"], dtype=_dt_vstr)

        # > initialize data structures for each observable
        if single_file is None:
            # > separate files for each observable
            for obs in in_files:
                if not in_files[obs]:
                    continue  # skip if no files for this observable yet
                h5grp_obs: h5py.Group = h5grp_pt[obs]
                nx: int = h5grp_obs.attrs["nx"]

                if "data" not in h5grp_obs:
                    # > create the data structure for this observable
                    xval: list[list[np.float64]] = []
                    ncols: int = 0
                    nrows: int = 0
                    with open(base_path / in_files[obs][0]) as dat_file:
                        for line in dat_file:
                            line = line.strip()
                            if not line:
                                continue  # skip empty lines
                            if line.startswith("#"):
                                parsed_nx = _parse_nx_marker(line)
                                if parsed_nx is not None:
                                    if parsed_nx != nx:
                                        raise ValueError(
                                            f"nx mismatch in {base_path / in_files[obs][0]}: "
                                            f"{parsed_nx} != {nx}"
                                        )
                                elif line.startswith("#overflow"):
                                    nrows += 1
                                    xval.append([np.float64(np.nan) for _ in range(nx)])
                                elif line.startswith("#labels"):
                                    h5grp_obs.attrs.create("labels", line, dtype=_dt_vstr)
                            else:
                                arr_f64 = np.fromstring(line, dtype=np.float64, sep=" ")
                                nrows += 1
                                xval.append(arr_f64[:nx])
                                ncols_: int = len(arr_f64) - nx
                                assert ncols_ % 2 == 0
                                ncols_ = ncols_ // 2  # pairs of: (val,err) in columns
                                if ncols == 0:
                                    ncols = ncols_
                                else:
                                    assert ncols == ncols_
                    # > create empty datasets for this observable
                    _ = h5grp_obs.create_dataset("files", (0,), dtype=_dt_vstr, maxshape=(None,))
                    # > neval is per-job (not per-bin): store as 1-D to avoid nrows*ncols redundancy
                    _ = h5grp_obs.create_dataset("neval", (0,), dtype=np.int64, maxshape=(None,))
                    h5dat_data = h5grp_obs.create_dataset(
                        "data",
                        (nrows, ncols, 0),
                        dtype=_dt_hist,
                        maxshape=(nrows, ncols, None),
                        chunks=(1, 1, _chunk_size),  # read pattern: [irow, icol, :] → align last axis
                        compression="lzf",  # faster (de-)compression, only for h5py
                    )
                    if nx > 0:
                        h5dat_xval = h5grp_obs.create_dataset(
                            "xval", (nrows, nx), dtype=np.float64, data=xval
                        )
                        h5dat_xval.make_scale("x value")
                        h5dat_data.dims[0].attach_scale(h5dat_xval)

                # > all structures exist at this point
                # > time to populate new data
                h5dat_files: h5py.Dataset = h5grp_obs["files"]
                h5dat_neval: h5py.Dataset = h5grp_obs["neval"]
                h5dat_data: h5py.Dataset = h5grp_obs["data"]
                nrows, ncols, ndat_phys = h5dat_data.shape
                ndat_old: int = int(h5grp_obs.attrs.get("ndat_valid", ndat_phys))
                if nx > 0:
                    xval = h5dat_data.dims[0][0][...]

                in_files_old: list[GenericPath] = [file_path for file_path in h5dat_files.asstr()[:ndat_old]]
                in_files_cur: list[GenericPath] = list(dict.fromkeys(in_files[obs]))
                in_files_new: list[GenericPath] = [
                    file_path for file_path in in_files_cur if file_path not in in_files_old
                ]
                ndat_new: int = len(in_files_new)
                if ndat_new == 0 or (h5grp_obs.attrs["timestamp"] < 0 and not merge_in_progress):
                    continue
                h5grp_obs.attrs["timestamp"] = -1.0  # flag merging state
                # > resize datasets to accommodate new files (only extends, never shrinks)
                ndat_total: int = ndat_old + ndat_new
                resize_obs[obs] = ndat_old
                h5dat_files.resize((max(ndat_total, ndat_phys),))
                h5dat_neval.resize((max(ndat_total, ndat_phys),))
                h5dat_data.resize((nrows, ncols, max(ndat_total, ndat_phys)))
                # > pre-allocate buffers once per observable
                # > layout (chunk_size, nrows, ncols): buf_data["result"][i, irow, :] is
                # > contiguous on the last axis during parse
                buf_data = np.empty((_chunk_size, nrows, ncols), dtype=_dt_hist)
                buf_neval = np.empty(_chunk_size, dtype=np.int64)
                for chunk_start in range(0, ndat_new, _chunk_size):
                    chunk_slice = in_files_new[chunk_start : chunk_start + _chunk_size]
                    chunk_len = len(chunk_slice)
                    # > parse job files into in-memory buffer
                    for i, ifile in enumerate(chunk_slice):
                        h5dat_files[ndat_old + chunk_start + i] = ifile
                        with open(base_path / ifile) as dat_file:
                            lines = dat_file.read().splitlines()
                        buf_neval[i] = -1
                        data_lines: list[str] = []
                        data_irows: list[int] = []
                        irow: int = 0
                        for line in lines:
                            line = line.strip()
                            if not line:
                                continue
                            if line.startswith("#"):
                                parsed_nx = _parse_nx_marker(line)
                                if parsed_nx is not None:
                                    if parsed_nx != nx:
                                        raise ValueError(
                                            f"nx mismatch in {base_path / ifile}: {parsed_nx} != {nx}"
                                        )
                                elif line.startswith("#neval"):
                                    buf_neval[i] = int(line.split()[-1])
                                elif line.startswith("#overflow"):
                                    # > overflow rows are rare: parse individually
                                    arr_f64 = np.fromstring(
                                        line.split(None, nx)[nx], dtype=np.float64, sep=" "
                                    )
                                    assert len(arr_f64) == 2 * ncols
                                    buf_data["result"][i, irow, :] = arr_f64[0::2]
                                    buf_data["error2"][i, irow, :] = arr_f64[1::2] ** 2
                                    irow += 1
                            else:
                                assert len(line.split()) == nx + 2 * ncols
                                data_lines.append(line)
                                data_irows.append(irow)
                                irow += 1
                        # > batch-parse all regular data lines in one fromstring call
                        if data_lines:
                            arr = np.fromstring(" ".join(data_lines), dtype=np.float64, sep=" ").reshape(
                                len(data_lines), nx + 2 * ncols
                            )
                            if nx > 0:
                                if len(data_lines) == nrows:  # no overflow row
                                    assert np.array_equal(arr[:, :nx], xval)
                                else:
                                    assert np.array_equal(arr[:, :nx], xval[data_irows])
                            if len(data_lines) == nrows:
                                # > no overflow rows: direct field-view assignment
                                buf_data["result"][i] = arr[:, nx::2]
                                buf_data["error2"][i] = arr[:, nx + 1 :: 2] ** 2
                            else:
                                # > overflow rows present: scatter data rows back to their irow
                                for k, irow_k in enumerate(data_irows):
                                    buf_data["result"][i, irow_k, :] = arr[k, nx::2]
                                    buf_data["error2"][i, irow_k, :] = arr[k, nx + 1 :: 2] ** 2
                        assert irow == nrows
                    # > single 3D write: transpose to match HDF5 layout
                    # > to match HDF5 chunk layout; ascontiguousarray ensures one contiguous copy
                    idat_start = ndat_old + chunk_start
                    idat_end = idat_start + chunk_len
                    h5dat_neval[idat_start:idat_end] = buf_neval[:chunk_len]
                    h5dat_data[:, :, idat_start:idat_end] = np.ascontiguousarray(
                        buf_data[:chunk_len].transpose(1, 2, 0)
                    )
                    resize_obs[obs] += chunk_len
                expected_nfiles = len(set(in_files_old).union(in_files_cur))
                assert resize_obs[obs] == expected_nfiles
                # > update ndat_valid as the final step — crash before this leaves
                # > old ndat_valid intact, excess data invisible, next run re-appends idempotently
                h5grp_obs.attrs["ndat_valid"] = resize_obs[obs]
                # > stable input timestamp for MergeObs freshness checks: max mtime across
                # > all stored files (old + new).  Never use wall-clock here — coarse-grained
                # > filesystem mtimes can make a freshly-written .dat look stale on fast resumes.
                h5grp_obs.attrs["timestamp"] = max(
                    (base_path / file_path).stat().st_mtime
                    for file_path in set(in_files_old) | set(in_files_cur)
                )

        else:
            # > single_file is not None
            raise NotImplementedError("single_file option not implemented yet")
    return resize_obs


class MergeObs(Task):
    hdf5_in: GenericPath = luigi.Parameter()  # type: ignore[assignment]
    hdf5_path: list[str] = luigi.ListParameter()  # type: ignore[assignment]  # path to the observable group
    dat_out: GenericPath = luigi.Parameter()  # type: ignore[assignment]
    wgt_out: GenericPath | None = luigi.OptionalParameter(default=None)  # type: ignore[assignment]  # weights table output (required by `grids`, also usable standalone)
    # > propagated from MergePart: invalidates dat files older than the tag so config-driven
    # > recomputation (e.g. new trim_threshold) actually re-runs the merge core, not just the DB stamp
    reset_tag: float = luigi.FloatParameter(default=0.0)  # type: ignore[assignment]
    grids: bool = luigi.BoolParameter(default=False)  # type: ignore[assignment]

    priority = 130

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_hdf5: Path = self._path / self.hdf5_in
        self.file_dat: Path = self._path / self.dat_out
        self.file_wgt: Path | None = self._path / self.wgt_out if self.wgt_out is not None else None
        # > sidecar recording which HDF5 contents the current `.dat` was merged from; see `complete()`
        self.file_record: Path = self.file_dat.with_suffix(".merged.json")
        # > grid merging produces (and thus requires) a weights output; reject the contradictory
        # > combination up front so `required_outputs()`/`complete()` cannot mark such a task done.
        if self.grids and self.file_wgt is None:
            raise ValueError("MergeObs:  grid merging (grids=True) requires a weights output (wgt_out)")
        if not self.file_hdf5.is_file():
            raise FileNotFoundError(f"MergeObs:  HDF5 input file {self.file_hdf5} does not exist!")

    # > limit the resources on local cores
    @property
    def resources(self):  # type: ignore
        # return super().resources | {"local_ncores": 1, "MergeObs": 1}
        return super().resources | {"local_ncores": 1}

    def _hdf5_identity(self) -> tuple[int, float]:
        """Return the observable group's identity `(ndat_valid, version token)`.

        Read fresh on every call — Luigi may reuse the same task instance across
        multiple `complete()`/`run()` checks, so caching would give stale results
        after `MergePart` appends new data to the HDF5 group.
        """
        with h5py.File(self.file_hdf5, "r", libver="latest", swmr=True) as h5f:
            h5grp_obs = h5f["/".join(self.hdf5_path)]
            src_ts = (
                float(h5grp_obs.attrs["timestamp"])
                if "timestamp" in h5grp_obs.attrs
                else self.file_hdf5.stat().st_mtime
            )
            if "data" in h5grp_obs:
                ndat = int(h5grp_obs.attrs.get("ndat_valid", h5grp_obs["data"].shape[2]))
            else:
                ndat = 0
        return ndat, src_ts

    def _merge_settings(self) -> dict:
        """The merge-algorithm settings that influence `run()`'s output.

        Recorded in the sidecar so that, *whenever this MergeObs is (re)evaluated*, a
        config change (e.g. a new `trim_threshold`) re-runs the merge even if the HDF5
        inputs are unchanged.  Note this only bites once `MergePart` actually dispatches
        the MergeObs; in the workflow, config changes are propagated via `reset_tag`
        (fresh submit / finalize), and `MergePart.complete()` may short-circuit a fully
        merged part before then — so this digest is a safety net, not the primary trigger.
        """
        merge = self.config["merge"]
        return {
            "trim_threshold": merge["trim_threshold"],
            "trim_max_fraction": merge["trim_max_fraction"],
            "k_scan_nsteps": merge["k_scan_nsteps"],
            "k_scan_maxdev_steps": merge["k_scan_maxdev_steps"],
        }

    def required_outputs(self) -> list[Path]:
        """The output files that must exist for this merge to be complete.

        The merged `.dat` always; the weights table whenever a `wgt_out` was requested
        (`run()` always writes it then — used standalone for `[Options] weights=True`,
        not only for grids); and the PineAPPL grid when grid merging is enabled.
        `complete()` and the `MergePart` stall guard share this so they agree on what
        "the artifacts are present" means.
        """
        outputs = [self.file_dat]
        if self.file_wgt is not None:
            outputs.append(self.file_wgt)
        if self.grids:
            outputs.append(self.file_dat.with_suffix(".pineappl.lz4"))
        return outputs

    def expected_identity(self) -> tuple:
        """The freshness identity the current inputs/config *expect* the outputs to have.

        Everything that, if changed, would make a re-merge produce different artifacts;
        deliberately excludes on-disk state. The `MergePart` stall guard fingerprints this
        across run() restarts to detect a non-converging (stuck-predicate) merge.
        """
        ndat, src_ts = self._hdf5_identity()
        return (
            ndat,
            src_ts,
            float(self.reset_tag),
            tuple(sorted(self._merge_settings().items())),
            bool(self.grids),
            self.file_wgt is not None,
        )

    def _read_merge_record(self) -> dict | None:
        """Return the validated sidecar dict, or None if absent/unreadable/malformed.

        A sidecar that is not an object, or whose known fields have the wrong type
        (reject bool as a numeric field, and NaN/inf for `src_ts`/`reset_tag`), reads
        as "no usable metadata" so `complete()` simply forces a clean re-merge.
        """
        meta = read_json_sidecar(self.file_record)
        if not isinstance(meta, dict):
            return None
        if (
            type(meta.get("ndat", 0)) is not int
            or not is_finite_number(meta.get("src_ts", 0.0))
            or not is_finite_number(meta.get("reset_tag", 0.0))
            or not isinstance(meta.get("cfg", {}), dict)
            or type(meta.get("grids", False)) is not bool
            or type(meta.get("weights", False)) is not bool
        ):
            return None
        return meta

    def _write_merge_record(self, ndat: int, src_ts: float) -> None:
        """Record the full artifact identity `complete()` keys off (no mtime is consulted)."""
        write_json_sidecar(
            self.file_record,
            {
                "ndat": int(ndat),
                "src_ts": float(src_ts),
                "reset_tag": float(self.reset_tag),
                "cfg": self._merge_settings(),
                "grids": bool(self.grids),
                "weights": self.file_wgt is not None,
            },
        )

    def _freshness_reasons(self) -> list[str]:
        """Reasons the merged outputs are not up to date; an empty list means complete.

        Single source of truth for freshness, shared by `complete()` and
        `describe_incomplete()` so the predicate and its diagnostic can never drift.
        Decided entirely from the sidecar identity, never from output mtimes (mtime
        comparisons across the HDF5/`.dat` boundary are unreliable on networked
        filesystems and previously wedged `MergePart` in an endless re-merge loop).
        """
        reasons: list[str] = []
        # > all required artifacts (`.dat`, plus weights + grid when requested) must exist
        if missing := [str(p) for p in self.required_outputs() if not p.is_file()]:
            reasons.append(f"missing outputs={missing}")
        meta = self._read_merge_record()
        if meta is None:
            reasons.append("sidecar missing or unreadable")
            return reasons  # nothing to compare against
        # > forced re-merge: a newer reset epoch (config change / fresh submission / finalize)
        # > or a change in the merge-algorithm settings invalidates an older `.dat`.
        if float(meta.get("reset_tag", float("-inf"))) < self.reset_tag:
            reasons.append(f"reset_tag stale (sidecar={meta.get('reset_tag')} < task={self.reset_tag})")
        if meta.get("cfg") != self._merge_settings():
            reasons.append(f"merge-config drift (sidecar={meta.get('cfg')} != {self._merge_settings()})")
        # > weights/grids must have been produced for this identity (existence is covered above):
        # > guards a stale leftover weights file satisfying existence after a no-weights re-merge.
        if self.file_wgt is not None and not meta.get("weights", False):
            reasons.append("weights requested but sidecar weights=false")
        if self.grids and not meta.get("grids", False):
            reasons.append("grids requested but sidecar grids=false")
        # > new data: the `.dat` must reflect the current HDF5 group contents (entry count +
        # > version token), an equality of opaque tokens read from the *same* source.
        cur_ndat, cur_src_ts = self._hdf5_identity()
        if meta.get("ndat") != cur_ndat or meta.get("src_ts") != cur_src_ts:
            reasons.append(
                f"HDF5 state changed (sidecar ndat/src_ts={meta.get('ndat')}/{meta.get('src_ts')}"
                f" != current {cur_ndat}/{cur_src_ts})"
            )
        return reasons

    def complete(self) -> bool:
        return not self._freshness_reasons()

    def describe_incomplete(self) -> str:
        """Human-readable reason(s) `complete()` is False, for the stall-guard diagnostic."""
        reasons = self._freshness_reasons()
        return "; ".join(reasons) if reasons else "complete() is True (no mismatch)"

    def run(self):  # type: ignore[override]
        trim_threshold: float = self.config["merge"]["trim_threshold"]
        trim_max_fraction: float = self.config["merge"]["trim_max_fraction"]
        k_scan_nsteps: int = self.config["merge"]["k_scan_nsteps"]
        k_scan_maxdev_steps: float = self.config["merge"]["k_scan_maxdev_steps"]
        with h5py.File(self.file_hdf5, "r", libver="latest", swmr=True) as h5f:
            h5grp_obs: h5py.Group = h5f["/".join(self.hdf5_path)]
            src_ts: float = (
                float(h5grp_obs.attrs["timestamp"])
                if "timestamp" in h5grp_obs.attrs
                else self.file_hdf5.stat().st_mtime
            )
            nx: int = h5grp_obs.attrs["nx"]
            h5dat_neval: h5py.Dataset = h5grp_obs["neval"]
            h5dat_data: h5py.Dataset = h5grp_obs["data"]
            nrows, ncols, ndat_phys = h5dat_data.shape
            ndat: int = int(h5grp_obs.attrs.get("ndat_valid", ndat_phys))
            # > neval is per-job (identical across all bins): read only the valid slice
            bin_neval: np.ndarray = h5dat_neval[:ndat]
            # > per-bin buffer reused across the (irow, icol) loop
            bin_data = np.empty((ndat,), dtype=_dt_hist)
            bin_cmlt = np.empty(
                (ndat + 1,), dtype=_dt_cmlt
            )  # one trailing entry to accumulate "trimmed" datasets
            bin_mask = np.empty(
                (ndat + 1,), dtype=np.int32
            )  # mask to keep track of trimmed data (0: active, 1: trimmed, 2: invalid, <0: merged)
            # > buffers for intermediate operations
            bin_buf1 = np.empty((ndat + 1,), dtype=np.float64)
            bin_buf2 = np.empty((ndat + 1,), dtype=np.float64)
            # > the final merged result; neval tracked separately as sum of all job nevals
            merged_hist = np.zeros((nrows, ncols), dtype=_dt_hist)
            neval_total: int = int(np.sum(bin_neval))
            weights = np.full((nrows, ndat), np.nan, dtype=np.float64) if self.file_wgt is not None else None

            # > more information needed for the output
            xval = h5dat_data.dims[0][0][...] if nx > 0 else None
            labels = h5grp_obs.attrs.get("labels", None)
            filenames = [str(self._local(f).absolute()) for f in h5grp_obs["files"].asstr()[:ndat]]

            def combine_unweighted() -> tuple[np.float64, np.float64]:
                # > unweigthed average as a reference
                nonlocal bin_cmlt, bin_mask
                _mask = bin_mask == BinMask.ACTIVE
                _neval = np.sum(bin_cmlt["neval"], where=_mask)
                _result = np.sum(bin_cmlt["sumf"], where=_mask) / _neval
                _error = np.sqrt(np.sum(bin_cmlt["sumf2"], where=_mask) - _result**2 * _neval) / _neval
                return _result, _error

            def combine_weighted() -> tuple[np.float64, np.float64]:
                # > compute the weighted average using the sumf arrays
                nonlocal bin_cmlt, bin_mask
                nonlocal bin_buf1, bin_buf2
                _mask = (bin_mask == BinMask.ACTIVE) & (bin_cmlt["sumf2"] > 0.0)
                bin_buf1[:] = 0
                bin_buf2[:] = 0
                np.square(bin_cmlt["sumf"], out=bin_buf1, where=_mask)
                np.divide(bin_buf1, bin_cmlt["neval"], out=bin_buf1, where=_mask)
                np.subtract(bin_cmlt["sumf2"], bin_buf1, out=bin_buf1, where=_mask)
                np.square(bin_cmlt["neval"], out=bin_buf2, where=_mask)
                np.divide(bin_buf2, bin_buf1, out=bin_buf1, where=_mask)
                _error = np.sum(bin_buf1[_mask])
                if _error > 0.0:
                    np.divide(bin_cmlt["sumf"], bin_cmlt["neval"], out=bin_buf2, where=_mask)
                    np.multiply(bin_buf2, bin_buf1, out=bin_buf2, where=_mask)
                    _result = np.sum(bin_buf2, where=_mask) / _error
                    _error = 1.0 / np.sqrt(_error)
                else:
                    _result = np.float64(0.0)
                    _error = np.float64(0.0)
                return _result, _error

            def merge_pair() -> None:
                # > small & large stats to average out differences in (pseudo-)job statistics
                nonlocal bin_cmlt, bin_mask
                _ibuf = np.argsort(bin_cmlt["neval"])
                _mask = bin_mask == BinMask.ACTIVE
                nstart = np.sum(_mask)
                # > init indices
                ilow: int = 0
                iupp: int = ndat
                low: int = _ibuf[ilow]
                upp: int = _ibuf[iupp]
                # > loop over pairs
                while ilow < iupp:
                    # > skip invalid lower
                    while ilow < ndat and not _mask[low]:
                        ilow += 1
                        low = _ibuf[ilow]
                    # > skip invalid upper
                    while iupp > 0 and not _mask[upp]:
                        iupp -= 1
                        upp = _ibuf[iupp]
                    # > out of pairs to merge
                    if ilow >= iupp:
                        break
                    # > unweighted combinations of two (pseudo-)runs
                    # > we always absorb the lower index into the higher one
                    # > this ensures that index `0` either remains ACTIVE or is merged
                    # > and we can use negative indices to keep track of merge history
                    low, upp = sorted((low, upp))  # reset below
                    bin_cmlt["neval"][upp] += bin_cmlt["neval"][low]
                    bin_cmlt["sumf"][upp] += bin_cmlt["sumf"][low]
                    bin_cmlt["sumf2"][upp] += bin_cmlt["sumf2"][low]
                    bin_cmlt[low] = 0  # reset
                    bin_mask[low] = -upp
                    _mask[low] = False
                    # > move to next pair
                    ilow += 1
                    iupp -= 1
                    low = _ibuf[ilow]
                    upp = _ibuf[iupp]
                nend = np.sum(_mask)
                assert nend <= nstart

            for irow in range(nrows):
                for icol in range(ncols):
                    # > populate the arrays to perform the merge
                    h5dat_data.read_direct(bin_data, source_sel=np.s_[irow, icol, :ndat])
                    # > we operate on the f & f2 cumulants from here on, leave `bin_data` alone
                    bin_cmlt[:] = 0
                    bin_cmlt["neval"][:ndat] = bin_neval
                    np.multiply(bin_neval, bin_data["result"], out=bin_cmlt["sumf"][:ndat])
                    bin_buf1[:] = 0
                    bin_buf2[:] = 0
                    np.square(bin_neval, out=bin_buf1[:ndat])
                    np.multiply(bin_data["error2"], bin_buf1[:ndat], out=bin_buf1[:ndat])
                    np.square(bin_data["result"], out=bin_buf2[:ndat])
                    np.multiply(bin_neval, bin_buf2[:ndat], out=bin_buf2[:ndat])
                    np.add(bin_buf1[:ndat], bin_buf2[:ndat], out=bin_cmlt["sumf2"][:ndat])

                    # > some cleanup & flagging of invalid entries
                    bin_mask[:] = BinMask.ACTIVE  # switch on all entries
                    bin_mask[ndat] = BinMask.INVALID  # "trimmed" entry not yet populated
                    bin_mask[:ndat][~np.isfinite(bin_data["result"])] = (
                        BinMask.INVALID
                    )  # discard all non-finite results (nan, +/- inf)
                    bin_mask[:ndat][bin_neval <= 0] = (
                        BinMask.INVALID
                    )  # discard all entries with zero evaluations
                    bin_cmlt[bin_mask == BinMask.INVALID] = 0
                    # > error = zero should only happen if result is also zero
                    assert np.all(bin_data["result"][bin_data["error2"] == 0.0] == 0.0)

                    # > apply outlier trimming
                    # > a two-sided ("double") MAD is used instead of a single, symmetric
                    # > scale: the per-job result distribution can be strongly skewed (heavy
                    # > tailed event weights), so estimating the robust 1-sigma separately
                    # > below and above the median avoids biasing the rejection towards the
                    # > longer tail. MAD is preferred over the IQR as it maps onto a z-score.
                    _mask = (bin_mask == BinMask.ACTIVE) & (
                        bin_cmlt["sumf2"] > 0.0
                    )  # exclude "zero bins" from being trimmed
                    n_active = int(np.sum(_mask))
                    if trim_threshold > 0.0 and n_active > 1:
                        # > `_mask[ndat]` is INVALID here, so `_mask[:ndat]` selects the same jobs
                        res = bin_data["result"][_mask[:ndat]]
                        dev = res - np.median(res)
                        below = dev < 0.0
                        above = dev > 0.0
                        # > robust 1-sigma scale on each side of the median (NaN-safe via `.any()`)
                        scale_lo = float(np.median(-dev[below])) / _MAD_NORMAL_SCALE if below.any() else 0.0
                        scale_hi = float(np.median(dev[above])) / _MAD_NORMAL_SCALE if above.any() else 0.0
                        # > fall back to the populated side if one half-sample has no spread
                        scale_lo = scale_lo or scale_hi
                        scale_hi = scale_hi or scale_lo
                        if scale_lo > 0.0 and scale_hi > 0.0:
                            # > side-aware robust z-score, weighted by the per-job statistics:
                            # > a better-sampled job (larger neval) is penalised more for the same
                            # > offset, i.e. trim when `|dev| / sigma * sqrt(neval / <neval>)` is large
                            avg_neval = np.sum(bin_cmlt["neval"][_mask]) / (n_active + 0.1)
                            bin_buf1[:] = 0.0  # `ndat` entry stays 0, so it sorts last and is never trimmed
                            bin_buf1[_mask] = (
                                np.abs(dev)
                                / np.where(below, scale_lo, scale_hi)
                                * np.sqrt(bin_cmlt["neval"][_mask] / avg_neval)
                            )
                            # > trim the most significant offsets first, stopping once we drop below
                            # > the threshold or reach the maximum fraction of jobs we may trim
                            max_trim = trim_max_fraction * ndat
                            for ntrim, itrim in enumerate(np.argsort(-bin_buf1)):  # most significant first
                                if bin_buf1[itrim] <= trim_threshold or (ntrim + 1) > max_trim:
                                    break
                                bin_mask[itrim] = BinMask.TRIMMED
                            # > trimmed datasets are accumulated into a mega "outlier" dataset
                            # > which will eventually be suppressed in the weighted average by the large error
                            _mask = bin_mask == BinMask.TRIMMED
                            bin_cmlt["neval"][ndat] = np.sum(bin_cmlt["neval"][_mask])
                            bin_cmlt["sumf"][ndat] = np.sum(bin_cmlt["sumf"][_mask])
                            bin_cmlt["sumf2"][ndat] = np.sum(bin_cmlt["sumf2"][_mask])
                            bin_cmlt[_mask] = 0
                            bin_mask[ndat] = BinMask.INVALID  # keep it trimmed for now

                    # > weighted average cannot deal with "zero bins" but those jobs still matter
                    # > do a pairwise merge until there are no "zero bins" or only one pseudo-job is left
                    while True:
                        _mask = bin_mask == BinMask.ACTIVE
                        if np.sum(_mask) <= 1 or np.sum(bin_cmlt["sumf2"][_mask] == 0.0) <= 0:
                            break
                        merge_pair()

                    # > perform the k-scan
                    _neval = int(np.sum(bin_cmlt["neval"]))  # no mask(!) since err=0 events also count
                    k_scan: list[tuple[np.float64, np.float64, np.int32]] = []
                    while True:
                        _result, _error = combine_weighted()
                        _mask = bin_mask == BinMask.ACTIVE
                        k_scan.append((_result, _error, np.sum(_mask)))
                        # > no k-scan active or nothing left to merge
                        if (k_scan_nsteps <= 0) or (np.sum(_mask) <= 1):
                            break
                        # > check for a plateau spanning the last k_scan_nsteps steps
                        qplateau: bool = len(k_scan) >= k_scan_nsteps  # enough steps?
                        for istep in range(-1, -k_scan_nsteps - 1, -1):
                            if not qplateau:
                                break
                            for jstep in range(istep - 1, -k_scan_nsteps - 1, -1):
                                delta = np.abs(k_scan[istep][0] - k_scan[jstep][0])
                                sigma = np.sqrt(k_scan[istep][1] ** 2 + k_scan[jstep][1] ** 2)
                                # > each step uses identical data so standard variance is not suitable
                                if delta > k_scan_maxdev_steps * sigma:
                                    qplateau = False
                                    break
                        if qplateau:
                            break
                        # > prepare for the next step (pair up two pseudoruns into a single one)
                        merge_pair()

                    merged_hist[irow, icol] = k_scan[-1][:2]
                    # > determine weights (only for the "central" prediction)
                    if weights is not None and icol == 0:
                        for idat in range(ndat):
                            if (bin_mask[idat] == BinMask.INVALID) or (bin_mask[idat] == BinMask.TRIMMED):
                                weights[irow, idat] = 0.0
                            elif bin_mask[idat] == BinMask.ACTIVE:
                                # > the merged/absorbed data will be set in this "parent" active case
                                # > the weight from the weighted average
                                _neval, _sumf, _sumf2 = bin_cmlt[idat]
                                _ierr2 = (_sumf2 - _sumf**2 / _neval) / _neval**2
                                if _ierr2 <= 0.0:
                                    # > near-constant integrand: floating-point rounding makes the
                                    # > variance estimate non-positive even though Σf² > 0.
                                    # > combine_weighted() may still return a small non-zero merged
                                    # > error from other active bins, so we cannot assert
                                    # > merged_hist["error2"] == 0. Assign zero weight instead.
                                    _iwgt = 0.0
                                else:
                                    _iwgt = (1.0 / _ierr2) * merged_hist[irow, icol]["error2"] ** 2
                                # > find all "nodes" that were merged into `idat`
                                _inode: int = 0
                                _nodes_list = [idat]
                                while _inode < len(_nodes_list):
                                    # > find all children of the current node
                                    if _nodes_list[_inode] != 0:
                                        _nodes_list.extend(
                                            np.flatnonzero(bin_mask == -_nodes_list[_inode]).tolist()
                                        )
                                    _inode += 1
                                _nodes = np.asarray(_nodes_list, dtype=int)
                                for _inode in _nodes:
                                    weights[irow, _inode] = _iwgt * bin_neval[_inode] / _neval
                        assert np.all(
                            np.isfinite(weights[irow, :])
                        )  # check that we have weights for all entries

        _write_dat(self.file_dat, labels, neval_total, nx, xval, merged_hist)
        # > Forward-stamp the `.dat` mtime past now/src_ts/reset_tag for the benefit of any
        # > downstream consumer that orders outputs by mtime (e.g. the standalone combine stages).
        # > `complete()` no longer reads this mtime — freshness is tracked entirely by the sidecar.
        complete_mtime = max(time.time(), src_ts, self.reset_tag) + 1.0
        os.utime(self.file_dat, (complete_mtime, complete_mtime))

        if self.file_wgt is not None and weights is not None:
            _write_weights(self.file_wgt, nx, xval, filenames, weights)

            if self.grids:
                pine_merge: Path = Path(self.config["exe"]["path"]).parent / "nnlojet-merge-pineappl"
                if not pine_merge.is_file() or not os.access(pine_merge, os.X_OK):
                    raise RuntimeError(f"Missing nnlojet-merge-pineappl executable at {pine_merge}")
                grid_file = self.file_dat.with_suffix(".pineappl.lz4")
                _run_pineappl_merge(pine_merge, self.file_wgt, grid_file, check=True)
        elif self.grids:
            raise RuntimeError("Grid merging requires a weights output file")

        # > record the full artifact identity (HDF5 inputs, reset epoch, merge config, grids) as
        # > the final step, once every output is on disk.  `complete()` compares this against the
        # > live state, so a crash before here simply leaves the task incomplete and it re-merges.
        self._write_merge_record(ndat, src_ts)
