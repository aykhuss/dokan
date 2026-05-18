"""dokan merge tasks

defines tasks to merge individual NNLOJET results into a combined result.
constitutes the dokan workflow implementation of `nnlojet-combine.py`
"""

import datetime
import json
import math
import os
import re
import shutil
import subprocess
import time
from abc import ABCMeta
from enum import IntEnum, unique
from pathlib import Path

import h5py
import luigi
import numpy as np
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .._types import GenericPath
from ..combine import NNLOJETHistogram
from ..exe._exe_config import ExecutionMode
from ..exe._exe_data import ExeData
from ..order import Order
from ..task import Task
from ..util import format_time_interval
from ._dbtask import DBTask
from ._jobstatus import JobStatus
from ._loglevel import LogLevel
from ._sqla import Job, Log, Part

# > some variable definitions
_dt_vstr = h5py.string_dtype()
_dt_hist = np.dtype([("result", np.float64), ("error2", np.float64)])  # HDF5 storage: per-job, per-bin
_dt_cmlt = np.dtype([("neval", np.int64), ("sumf", np.float64), ("sumf2", np.float64)])  # in-memory cumulants
_chunk_size: int = 256  # chunk size along the ndat axis
_MAD_NORMAL_SCALE: float = 0.6745  # median absolute deviation to 1-sigma for a normal distribution


def _obs_has_grid(hist_info: dict) -> bool:
    """Return whether this observable has an associated PineAPPL grid."""
    return hist_info.get("grid") is not None


@unique
class BinMask(IntEnum):
    """possible values for the bin mask"""

    ACTIVE = 0
    TRIMMED = 1
    INVALID = 2


class DBMerge(DBTask, metaclass=ABCMeta):
    # > flag to force a re-merge (if new jobs are in a `done` state but not yet `merged`)
    force: bool = luigi.BoolParameter(default=False)  # type: ignore[assignment]
    # > tag to trigger a reset to initiate a re-merge from scratch (timestamp)
    reset_tag: float = luigi.FloatParameter(default=0.0)  # type: ignore[assignment]
    # > flag to trigger write-out of weights for interpolation grids
    grids: bool = luigi.BoolParameter(default=False)  # type: ignore[assignment]

    priority = 120

    # > limit the resources on local cores
    @property
    def resources(self):  # type: ignore
        return super().resources | {"local_ncores": 1}

    def _make_prefix(self, session: Session | None = None) -> str:
        return (
            self.__class__.__name__
            + "["
            + ", ".join(
                ([f"force={self.force}"] if self.force else [])
                + ([f"reset={time.ctime(self.reset_tag)}"] if self.reset_tag > 0.0 else [])
            )
            + "]"
        )


class MergeObs(Task):
    hdf5_in: GenericPath = luigi.Parameter()  # type: ignore[assignment]
    hdf5_path: list[str] = luigi.ListParameter()  # type: ignore[assignment]  # path to the observable group
    dat_out: GenericPath = luigi.Parameter()  # type: ignore[assignment]
    wgt_out: GenericPath | None = luigi.OptionalParameter(default=None)  # type: ignore[assignment]  # only used if `grids` is True
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
        if not self.file_hdf5.is_file():
            raise FileNotFoundError(f"MergeObs:  HDF5 input file {self.file_hdf5} does not exist!")

    # > limit the resources on local cores
    @property
    def resources(self):  # type: ignore
        # return super().resources | {"local_ncores": 1, "MergeObs": 1}
        return super().resources | {"local_ncores": 1}

    def complete(self):
        if not self.file_dat.is_file():
            return False
        dat_mtime = self.file_dat.stat().st_mtime
        if dat_mtime < self.reset_tag:
            return False
        # > read source timestamp fresh on every call — Luigi may reuse the same task instance
        # > across multiple complete() checks, so caching in __init__ would give stale results
        # > after MergePart appends new data to the HDF5 group.
        with h5py.File(self.file_hdf5, "r", libver="latest", swmr=True) as h5f:
            h5grp_obs = h5f["/".join(self.hdf5_path)]
            src_ts = (
                float(h5grp_obs.attrs["timestamp"])
                if "timestamp" in h5grp_obs.attrs
                else self.file_hdf5.stat().st_mtime
            )
        if dat_mtime < src_ts:
            return False

        if self.grids:
            if self.file_wgt is None or not self.file_wgt.is_file():
                return False
            grid_file = self.file_dat.with_suffix(".pineappl.lz4")
            if not grid_file.is_file():
                return False
            if grid_file.stat().st_mtime < self.file_wgt.stat().st_mtime:
                return False

        return True

    def run(self):  # type: ignore[override]
        # print(
        #     f"MergeObs:  {self.hdf5_in}:{self.hdf5_path} > {self.dat_out} & {self.wgt_out if self.wgt_out else '(no weights)'}"
        # )
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
                # print(f"{bin_cmlt["neval"]=}")
                # print(f"{_ibuf=}")
                # print(f"{_mask=}")
                # > init indices
                ilow: int = 0
                iupp: int = ndat
                low: int = _ibuf[ilow]
                upp: int = _ibuf[iupp]
                # > loop over pairs
                while ilow < iupp:
                    # print(f"  | start pair {ilow} ({low}) <-> {iupp} ({upp})")
                    # > skip invalid lower
                    while ilow < ndat and not _mask[low]:
                        # print(f"{ilow} ({low}): {_mask[low]=}")
                        ilow += 1
                        low = _ibuf[ilow]
                    # > skip invalid upper
                    while iupp > 0 and not _mask[upp]:
                        # print(f"{iupp} ({upp}): {_mask[upp]=}")
                        iupp -= 1
                        upp = _ibuf[iupp]
                    # print(f"  | first valid pair {ilow} ({low}) <-> {iupp} ({upp})")
                    # > out of pairs to merge
                    if ilow >= iupp:
                        # print(f"  | complete one iteration of pairwise merging: {ilow} >= {iupp}")
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
                    # print(f"  >  merged {low} into {upp}")
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
                    # X  bin_cmlt["sumf"][:ndat] = bin_neval * bin_data["result"]
                    np.multiply(bin_neval, bin_data["result"], out=bin_cmlt["sumf"][:ndat])
                    # X  bin_cmlt["sumf2"][:ndat] = bin_neval ** 2 * bin_data["error2"] + bin_neval * bin_data["result"] ** 2
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

                    # > appy outlier trimming
                    # > we'll use MAD instead of IQR as it is easier to convert to a standard z-score
                    _mask = (bin_mask == BinMask.ACTIVE) & (
                        bin_cmlt["sumf2"] > 0.0
                    )  # exclude "zero bins" from being trimmed
                    if trim_threshold > 0.0 and np.sum(_mask) > 1:
                        q25, q50, q75 = np.quantile(bin_data["result"][_mask[:ndat]], [0.25, 0.50, 0.75])
                        bin_buf1[:] = 0
                        bin_buf1[_mask] = np.abs(
                            bin_data["result"][_mask[:ndat]] - q50
                        )  # `ndat` entry invalid: no need for [:ndat] on lhs
                        mad = np.median(bin_buf1[_mask])
                        threshold = trim_threshold * (mad / _MAD_NORMAL_SCALE)  # convert to z-score
                        # > start trimming from the "worst" until we either run out or would exceed the max fraction
                        # > skip `[_mask]` since initialised to zero (makes indexing easier than for sliced arrays)
                        # X  bin_mask[bin_buf1 > threshold] = BinMask.TRIMMED
                        avg_neval = np.sum(bin_cmlt["neval"][_mask]) / (np.sum(_mask) + 0.1)
                        ntrim: int = 0
                        # for itrim in np.argsort(bin_buf1)[::-1]:
                        for itrim in np.argsort(-bin_buf1):  # largest defiation first
                            if bin_buf1[itrim] <= threshold:
                                break
                            if (ntrim + 1) > trim_max_fraction * ndat:
                                break
                            # > we correct for the fact that the data samples can be based on different statistics
                            if bin_buf1[itrim] > threshold * np.sqrt(avg_neval / bin_cmlt["neval"][itrim]):
                                bin_mask[itrim] = BinMask.TRIMMED
                                ntrim += 1
                                # print(f" > trim {irow},{icol} [{itrim}] {bin_buf1[itrim]:.3f} > {threshold * np.sqrt(avg_neval / bin_cmlt['neval'][itrim]):.3f} ({ntrim}/{ndat})")
                        # > we will not discard the trimmed datasets but actually accumulate them into a mega "outlier" dataset
                        # > which will eventually be suppressed in the weighted average by the large error
                        _mask = bin_mask == BinMask.TRIMMED
                        bin_cmlt["neval"][ndat] = np.sum(bin_cmlt["neval"][_mask])
                        bin_cmlt["sumf"][ndat] = np.sum(bin_cmlt["sumf"][_mask])
                        bin_cmlt["sumf2"][ndat] = np.sum(bin_cmlt["sumf2"][_mask])
                        bin_cmlt[_mask] = 0
                        bin_mask[ndat] = BinMask.ACTIVE if bin_cmlt["neval"][ndat] > 0 else BinMask.INVALID
                        # if bin_mask[ndat] == BinMask.ACTIVE:
                        #     print(f"trimmed {bin_cmlt['neval'][ndat]} [{irow},{icol}]")
                        bin_mask[ndat] = BinMask.INVALID  # keep it trimmed for now

                    # print(f"\n### {self.hdf5_path[0]}__{self.hdf5_path[1]}__{irow}__{icol}  active = {np.sum(bin_mask == BinMask.ACTIVE)}, non-zero = {np.sum(bin_cmlt['error2'] > 0.0)}")

                    # > weighted average cannot deal with "zero bins" but those jobs still matter and should not be discarded
                    # > do a pairwise merge of (pseudo-)jobs until there are no "zero bins" or there's only one psuedo-job left
                    while True:
                        _mask = bin_mask == BinMask.ACTIVE
                        if np.sum(_mask) <= 1 or np.sum(bin_cmlt["sumf2"][_mask] == 0.0) <= 0:
                            break
                        # print(f"  > merge {np.sum(bin_mask == BinMask.ACTIVE)} active bins, {np.sum(bin_cmlt['error2'] > 0.0)} non-zero bins")
                        merge_pair()

                    # > perform the k-scan
                    _neval = sum(bin_cmlt["neval"])  # no mask(!) since err=0 events also count
                    k_scan: list[tuple[np.float64, np.float64, np.int32]] = []
                    while True:
                        _result, _error = combine_weighted()
                        _mask = bin_mask == BinMask.ACTIVE
                        # print(f"#  appending {_result} +/- {_error} [{np.sum(_mask)}]")
                        k_scan.append((_result, _error, np.sum(_mask)))
                        # > no k-scan active or nothing left to merge
                        if (k_scan_nsteps <= 0) or (np.sum(_mask) <= 1):
                            # print(f"k-scan done: {np.sum(_mask)} pseudo-runs left")
                            break
                        # > check for a plateau spanning the last k_scan_nsteps steps
                        qplateau: bool = len(k_scan) >= k_scan_nsteps  # enough steps?
                        for istep in range(-1, -k_scan_nsteps - 1, -1):
                            if not qplateau:
                                break
                            for jstep in range(istep - 1, -k_scan_nsteps - 1, -1):
                                delta = np.abs(k_scan[istep][0] - k_scan[jstep][0])
                                sigma = np.sqrt(k_scan[istep][1] ** 2 + k_scan[jstep][1] ** 2)
                                # > each step is based on the identical dataset so just standard variance is not a suitable measure
                                # > taking the smaller of the two uncertainties better?
                                # sigma = max(abs(k_scan[istep][1]-k_scan[jstep][1]),min(k_scan[istep][1],k_scan[jstep][1]))
                                if delta > k_scan_maxdev_steps * sigma:
                                    # print(f"  X  {istep} <-> {jstep}: {delta:.6f} > {k_scan_maxdev_steps * sigma:.6f}")
                                    qplateau = False
                                    break
                        if qplateau:
                            # print("found plateau:")
                            # for r,e,n in k_scan:
                            #     print(f"  >> {r:.6f} +/- {e:.6f} ({n})")
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
                                    # print(f"  | {_inode}:{_nodes_list[_inode]} | {_nodes_list} | {np.flatnonzero(bin_mask == -_nodes_list[_inode])}")
                                    if _nodes_list[_inode] != 0:
                                        _nodes_list.extend(
                                            np.flatnonzero(bin_mask == -_nodes_list[_inode]).tolist()
                                        )
                                    _inode += 1
                                _nodes = np.asarray(_nodes_list, dtype=int)
                                # print(f" > nodes[{idat}]: {_nodes}")
                                for _inode in _nodes:
                                    weights[irow, _inode] = _iwgt * bin_neval[_inode] / _neval
                        # print(f" > weights: {weights[irow,:]}")
                        # print(f" > sum of weights [{irow}] = {np.sum(weights[irow, :]):.3f}")
                        assert np.all(
                            np.isfinite(weights[irow, :])
                        )  # check that we have weights for all entries
                        # if not np.all(np.isfinite(weights[irow, :])):
                        #     print(f" > mask: {bin_mask}")
                        #     time.sleep(5)

        with open(self.file_dat, "w") as df:
            if labels is not None:
                df.write(labels + "\n")
            df.write(f"#neval: {neval_total}\n")
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
                    df.write(f"{np.format_float_scientific(merged_hist['result'][irow, icol]): <25} ")
                    df.write(f"{np.format_float_scientific(merged_hist['error2'][irow, icol]): <25} ")
                df.write("\n")
        # > Make completion monotonic against both freshness gates.  Use +1.0 s so
        # > that filesystems with 1-second mtime resolution (NFS, Lustre) always
        # > floor to a value strictly >= src_ts, which can carry sub-second precision.
        complete_mtime = max(time.time(), src_ts, self.reset_tag) + 1.0
        os.utime(self.file_dat, (complete_mtime, complete_mtime))

        if self.file_wgt is not None and weights is not None:
            with open(self.file_wgt, "w") as wf:
                wf.write(f"#nx={nx} ")
                if xval is not None:
                    if nx == 3:
                        for irow in range(nrows):
                            if np.all(np.isnan(xval[irow])):
                                continue
                            wf.write(
                                f"[{np.format_float_scientific(xval[irow][0])},{np.format_float_scientific(xval[irow][-1])}] "
                            )
                wf.write("\n")
                for idat in range(ndat):
                    wf.write(filenames[idat] + " ")
                    for irow in range(nrows):
                        if xval is not None and np.all(np.isnan(xval[irow])):
                            continue
                        wf.write(np.format_float_scientific(weights[irow, idat]) + " ")
                    wf.write("\n")

            if self.grids:
                pine_merge: Path = Path(self.config["exe"]["path"]).parent / "nnlojet-merge-pineappl"
                if not pine_merge.is_file() or not os.access(pine_merge, os.X_OK):
                    raise RuntimeError(f"Missing nnlojet-merge-pineappl executable at {pine_merge}")

                grid_file = self.file_dat.with_suffix(".pineappl.lz4")
                grid_log = self.file_dat.with_suffix(".pineappl.log")
                job_env = os.environ.copy()

                with open(grid_log, "w") as log:
                    result = subprocess.run(
                        [
                            pine_merge,
                            str(self.file_wgt.relative_to(self.file_dat.parent)),
                            str(grid_file.relative_to(self.file_dat.parent)),
                            "-v",
                            "--skip",
                            "--noopt",
                        ],
                        env=job_env,
                        cwd=self.file_dat.parent,
                        stdout=log,
                        stderr=log,
                        text=True,
                    )
                    if result.returncode != 0:
                        raise RuntimeError(
                            f"nnlojet-merge-pineappl failed for {self.file_dat.name}. " + f"Check {grid_log}"
                        )
        elif self.grids:
            raise RuntimeError("Grid merging requires a weights output file")


class MergePart(DBMerge):
    # > merge only a specific `Part`
    part_id: int = luigi.IntParameter()  # type: ignore[assignment]

    @property
    def resources(self):  # type: ignore
        # return super().resources | {"local_ncores": 1, f"MergePart_{self.part_id}": 1}
        # > merge is I/O-bound (HDF5): skip local_ncores, use DBTask + per-part mutex
        return {"DBTask": 1, f"MergePart_{self.part_id}": 1}

    # @property
    # def select_part(self):
    #     return select(Part).where(Part.id == self.part_id).where(Part.active.is_(True))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger_prefix: str = "MergePart"
        with self.session as session:
            pt: Part = session.get_one(Part, self.part_id)
            self._logger_prefix = (
                self._logger_prefix
                + f"[{pt.name}"
                + (f", force={self.force}" if self.force else "")
                + (f", reset={time.ctime(self.reset_tag)}" if self.reset_tag > 0.0 else "")
                + "]"
            )
            self._debug(session, self._logger_prefix + "::init")

    @property
    def select_job(self):
        return (
            select(Job)
            .join(Part)
            .where(Part.id == self.part_id)
            .where(Part.active.is_(True))
            .where(Job.mode == ExecutionMode.PRODUCTION)
            .where(Job.status.in_(JobStatus.success_list()))
            # @todo: why did I have this? -> ".where(Job.timestamp < Part.timestamp)"
        )

    def complete(self) -> bool:
        with self.session as session:
            pt: Part = session.get_one(Part, self.part_id)

            if pt.timestamp < self.reset_tag:
                return False

            select_job_count = (
                select(func.count())
                .select_from(Job)
                .join(Part)
                .where(Part.id == self.part_id)
                .where(Part.active.is_(True))
                .where(Job.mode == ExecutionMode.PRODUCTION)
                .where(Job.status.in_(JobStatus.success_list()))
            )

            c_done = session.scalar(select_job_count.where(Job.status == JobStatus.DONE)) or 0
            c_merged = session.scalar(select_job_count.where(Job.status == JobStatus.MERGED)) or 0

            if (c_done + c_merged) == 0:
                self._debug(
                    session,
                    self._logger_prefix + f"::complete:  #done={c_done}, #merged={c_merged} => mark complete",
                )
                # @todo raise error as we should never be in this situation?
                return True

            self._debug(
                session,
                self._logger_prefix
                + f"::complete:  #done={c_done}, #merged={c_merged}, timestamp={time.ctime(pt.timestamp)}",
            )

            if self.force and c_done > 0:
                return False

            # > this is incorrect, as we need to wait for *all* pre-productions to be complete
            # > before we can merge. The merge is triggered manually in the `Entry` task
            # if c_merged == 0 and c_done > 0:
            #     return False

            # > only a pre-prduction
            # > still in pre-production stage: no merge (must force it: above)
            if (c_done == 1) and (c_merged <= 0):
                return True

            # > below min production number: force re-merge each time
            if (
                self.config["production"]["min_number"] > 0
                and c_done > 0
                and c_merged < self.config["production"]["min_number"]
            ):
                return False

            if (
                float(c_done + c_merged + 1) / float(c_merged + 1)
                < self.config["production"]["fac_merge_trigger"]
            ):
                return True

            self._debug(
                session,
                self._logger_prefix
                + f"::complete:  #done={c_done}, #merged={c_merged} => time for a re-merge",
            )

        return False

    def run(self):  # type: ignore[override]
        # Luigi restarts run() from the top after dynamic dependencies yielded
        # below complete.  If the part is already merged, returning here keeps
        # the DB timestamp stable and avoids invalidating a just-finished
        # MergeAll marker.
        if self.complete():
            with self.session as session:
                self._debug(session, self._logger_prefix + "::run:  already complete")
            return

        # > Phase 1: short DB session: collect job info, mark jobs MERGED, flag part as in-progress
        with self.session as session:
            pt: Part = session.get_one(Part, self.part_id)
            pt_name: str = pt.name
            merge_in_progress = pt.timestamp < 0.0
            self._logger(
                session, self._logger_prefix + "::run: " + ("fresh" if not merge_in_progress else "continue")
            )

            # > output directory
            mrg_path: Path = self._path.joinpath("result", "part", pt_name)
            if not mrg_path.exists():
                mrg_path.mkdir(parents=True)

            # > raw data path: need to move output files if not already moved
            if (raw_path := self.config["run"].get("raw_path")) is not None:
                raw_path = Path(raw_path)

            # > populate a dictionary with all histogram files (reduces IO)
            in_files: dict[str, list[GenericPath]] = dict()
            single_file: str | None = self.config["run"].get("histograms_single_file")
            if single_file is None:
                in_files = dict((obs, []) for obs in self.config["run"]["histograms"])
            else:
                in_files[single_file] = []  # all hist in single file
            # > collect histograms from all jobs
            pt.Ttot = 0.0
            pt.ntot = 0
            for job in session.scalars(self.select_job):
                if not job.rel_path:
                    continue  # @todo raise warning in logger?
                self._debug(session, self._logger_prefix + f"::run:  appending {job!r}")
                pt.Ttot += job.elapsed_time
                pt.ntot += job.niter * job.ncall
                job_path: Path = self._path / job.rel_path
                exe_data = ExeData(job_path)
                if raw_path is not None:
                    (raw_path / job.rel_path).mkdir(parents=True, exist_ok=True)

                for out in exe_data["output_files"]:
                    # > move to raw path
                    if raw_path is not None:
                        orig_file: Path = job_path / out
                        dest_file: Path = raw_path / job.rel_path / out
                        if orig_file.exists() and not orig_file.is_symlink():
                            shutil.move(orig_file, dest_file)
                            orig_file.symlink_to(dest_file)
                    if dat := re.match(r"^.*\.([^.]+)\.s[0-9]+\.dat", out):
                        if dat.group(1) in in_files:
                            in_files[dat.group(1)].append(str((job_path / out).relative_to(self._path)))
                        else:
                            self._logger(
                                session,
                                self._logger_prefix
                                + "::run:  "
                                + f"unmatched observable {dat.group(1)}?! ({in_files.keys()})",
                            )
                if not merge_in_progress:
                    job.status = JobStatus.MERGED
            if not merge_in_progress:
                # > this forces MergePart into an incomplete state
                # > that persists across the MergeObs yielding below
                pt.timestamp = -1.0
                self._safe_commit(session)
        # session closed — HDF5 I/O proceeds without holding a DB connection

        #############################
        # > Phase 2: HDF5 I/O: no DB session held
        # we create a separate file for each `Part` to allow for parallelised processing
        # * add a mask? -> no! MergeObs should only read
        # @todo refactor into separate member routine?
        # @todo: add move to `raw_path`
        # @todo: add compression (?works for vlen?); alternatively use fixed-size arrays with a copy-to-larger-shape-delete-original-rename workflow?
        # @todo: better to save sumf & sumf2? -> not so convenient for outliers and weighted avg but very convenient for unweighted combination and this also for the k-scan algorithm.
        # could start by storing res & err, then switch to sumf & sumf2 later when we want to apply the k-scan?
        # maybe an attribute to flag what of the two is stored in the datase? heler routine to convert between the two could also be nice.
        resize_max: int = max(len(files) for files in in_files.values()) if in_files else 0
        resize_obs: dict[str, int] = {}
        hdf5_file = self._path / "raw" / f"{pt_name}.hdf5"

        # > If Luigi resumed this task after yielding MergeObs, avoid touching
        # > the HDF5 file before checking MergeObs.complete(): its mtime is part
        # > of the freshness check for the generated .dat files.
        hdf5_obs_ready: set[str] = set()
        hdf5_obs_files: dict[str, set[GenericPath]] = {}
        if merge_in_progress and hdf5_file.is_file():
            with h5py.File(hdf5_file, "r", libver="latest", swmr=True) as h5f:
                if pt_name in h5f:
                    for obs in self.config["run"]["histograms"]:
                        if obs in h5f[pt_name] and "data" in h5f[pt_name][obs]:
                            h5grp = h5f[pt_name][obs]
                            nv = int(h5grp.attrs.get("ndat_valid", h5grp["data"].shape[2]))
                            if nv > 0:
                                hdf5_obs_ready.add(obs)
                                hdf5_obs_files[obs] = set(h5grp["files"].asstr()[:nv])

        resume_hdf5 = (
            merge_in_progress
            and bool(hdf5_obs_ready)
            and all(
                set(files).issubset(hdf5_obs_files.get(obs, set()))
                for obs, files in in_files.items()
                if files
            )
        )
        if resume_hdf5:
            with self.session as session:
                for job in session.scalars(self.select_job.where(Job.status == JobStatus.DONE)):
                    job.status = JobStatus.MERGED
                self._safe_commit(session)
        if not resume_hdf5:
            with h5py.File(hdf5_file, "a", libver="latest") as h5f:
                # > "single writer multiple reader" mode on for parallel reads
                h5f.swmr_mode = True

                # > retrieve top-level group; init group structure & data if needed
                h5grp_pt: h5py.Group = h5f.require_group(pt_name)

                # > make sure all observables groups are in place with the correct attributes
                for obs, hist in self.config["run"]["histograms"].items():
                    h5grp_obs: h5py.Group = h5grp_pt.require_group(f"{obs}")
                    if "timestamp" not in h5grp_obs.attrs:
                        h5grp_obs.attrs.create("timestamp", 0, dtype=np.float64)
                    if "nx" not in h5grp_obs.attrs:
                        h5grp_obs.attrs.create("nx", hist["nx"], dtype=np.int32)
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
                        nx = h5grp_obs.attrs["nx"]

                        if "data" not in h5grp_obs:
                            # > crate the data structure for this observable
                            xval: list[list[np.float64]] = []
                            ncols: int = 0
                            nrows: int = 0
                            with open(self._path / in_files[obs][0]) as dat_file:
                                for line in dat_file:
                                    line = line.strip()
                                    if not line:
                                        continue  # skip empty lines
                                    if line.startswith("#"):
                                        if line.startswith("#overflow"):
                                            nrows += 1
                                            xval.append([np.float64(np.nan) for _ in range(nx)])
                                        elif line.startswith("#nx"):
                                            assert int(line.split()[-1]) == nx
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

                        in_files_old: list[GenericPath] = [
                            file_path for file_path in h5dat_files.asstr()[:ndat_old]
                        ]
                        in_files_cur: list[GenericPath] = list(dict.fromkeys(in_files[obs]))
                        in_files_new: list[GenericPath] = [
                            file_path for file_path in in_files_cur if file_path not in in_files_old
                        ]
                        ndat_new: int = len(in_files_new)
                        if ndat_new == 0:
                            # print(f"{pt_name}[{obs}]: nothing to append ({ndat_new}/{len(in_files_old)})")
                            continue
                        elif h5grp_obs.attrs["timestamp"] < 0 and not merge_in_progress:
                            # print(f"{pt_name}[{obs}]: HDF5 in merging stage")
                            continue
                        else:
                            # print(f"{pt_name}[{obs}]: append {in_files_new} // {in_files_old}")
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
                                with open(self._path / ifile) as dat_file:
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
                                        if line.startswith("#neval"):
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
                                        elif line.startswith("#nx"):
                                            assert int(line.split()[-1]) == nx
                                    else:
                                        assert len(line.split()) == nx + 2 * ncols
                                        data_lines.append(line)
                                        data_irows.append(irow)
                                        irow += 1
                                # > batch-parse all regular data lines in one fromstring call
                                if data_lines:
                                    arr = np.fromstring(
                                        " ".join(data_lines), dtype=np.float64, sep=" "
                                    ).reshape(len(data_lines), nx + 2 * ncols)
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
                            (self._path / file_path).stat().st_mtime
                            for file_path in set(in_files_old) | set(in_files_cur)
                        )

                else:
                    # > single_file is not None
                    raise NotImplementedError("single_file option not implemented yet")

        # > find all obs that have data in the HDF5 file (may exceed resize_obs if results were deleted)
        hdf5_obs_ready: set[str] = set()
        hdf5_file = self._path / "raw" / f"{pt_name}.hdf5"
        with h5py.File(hdf5_file, "r") as h5f:
            if pt_name in h5f:
                for obs in self.config["run"]["histograms"]:
                    if obs in h5f[pt_name] and "data" in h5f[pt_name][obs]:
                        h5grp = h5f[pt_name][obs]
                        nv = int(h5grp.attrs.get("ndat_valid", h5grp["data"].shape[2]))
                        if nv > 0:
                            hdf5_obs_ready.add(obs)

        stale_grid_obs: set[str] = set()
        if self.grids:
            for obs in hdf5_obs_ready:
                hist_info = self.config["run"]["histograms"][obs]
                if not _obs_has_grid(hist_info):
                    continue
                dat_file = mrg_path / f"{obs}.dat"
                wgt_file = mrg_path / f"{obs}.weights.txt"
                grid_file = mrg_path / f"{obs}.pineappl.lz4"
                if (
                    not dat_file.exists()
                    or not wgt_file.exists()
                    or not grid_file.exists()
                    or grid_file.stat().st_mtime < wgt_file.stat().st_mtime
                ):
                    stale_grid_obs.add(obs)

        # > dispatch HDF5 file to MergeObs for each observable separately
        # > include obs with new data OR obs whose dat output is missing (e.g. results dir deleted)
        # > OR reset_tag active: forces re-run of MergeObs so config changes (trim, k-scan) take effect
        # > OR grid output is missing/stale when grid merging is enabled
        mrg_obs_dict = {
            obs: self.clone(
                cls=MergeObs,
                hdf5_in=str((self._path / "raw" / f"{pt_name}.hdf5").relative_to(self._path)),
                hdf5_path=[f"{pt_name}", f"{obs}"],
                dat_out=str((mrg_path / f"{obs}.dat").relative_to(self._path)),
                wgt_out=(
                    str((mrg_path / f"{obs}.weights.txt").relative_to(self._path))
                    if self.grids and _obs_has_grid(hist_info)
                    else None
                ),
                reset_tag=self.reset_tag,
                grids=self.grids and _obs_has_grid(hist_info),
            )
            for obs, hist_info in self.config["run"]["histograms"].items()
            if obs in resize_obs
            or (obs in hdf5_obs_ready and not (mrg_path / f"{obs}.dat").exists())
            or (self.reset_tag > 0.0 and obs in hdf5_obs_ready)
            or obs in stale_grid_obs
        }
        # with self.session as session:
        #     self._debug(
        #         session,
        #         self._logger_prefix
        #         + f"::run:  yield {[mrg_obs.dat_out for mrg_obs in mrg_obs_dict.values()]} for merging ...",
        #     )
        pending_mrg_obs = [mrg_obs for mrg_obs in mrg_obs_dict.values() if not mrg_obs.complete()]
        if pending_mrg_obs:
            yield pending_mrg_obs

        #############################
        # > Phase 3: post-yield cross-section computation: no DB session held
        # > update cross section estimates for the part & collect all estimates also from distributions
        cross_result: float = 0.0
        cross_error: float = 0.0
        cross_list: list[tuple[float, float]] = []
        # > update needs to loop over all histograms, not just the ones that were updated
        for obs in self.config["run"]["histograms"]:
            # print(f" post-processing observable {obs} ...")
            file_out: Path = mrg_path / f"{obs}.dat"
            if not file_out.exists():
                continue  # can happen when new histo added to `template.run`
            hist_info = self.config["run"]["histograms"][obs]
            nx: int = hist_info["nx"]

            # > register cross section numbers
            if "cumulant" in hist_info:
                continue  # @todo ?

            res, err = 0.0, 0.0  # accumulate bins to "cross" (possible fac, selectors, ...)
            if nx == 0:
                with open(file_out) as cross:
                    for line in cross:
                        if line.startswith("#"):
                            continue
                        col: list[float] = [float(c) for c in line.split()]
                        res = col[0]
                        err = col[1] ** 2
                        break
            elif nx == 3:
                with open(file_out) as diff:
                    for line in diff:
                        if line.startswith("#overflow"):
                            scol: list[str] = line.split()
                            res += float(scol[3])
                            err += float(scol[4]) ** 2
                        if line.startswith("#"):
                            continue
                        col: list[float] = [float(c) for c in line.split()]
                        res += (col[2] - col[0]) * col[3]
                        # > this is formally not the correct way to compute the error
                        # > but serves as a conservative error for optimizing on histograms
                        err += ((col[2] - col[0]) * col[4]) ** 2
            else:
                raise ValueError(self._logger_prefix + f"::run:  unexpected nx = {nx}")
            err = math.sqrt(err)

            if obs == "cross":
                cross_result = res
                cross_error = err

            cross_list.append((res, err))

        # > update the error from the chosen optimization target
        opt_target: str = self.config["run"]["opt_target"]

        # > different estimates for the relative cross uncertainties
        rel_cross_err: float = 0.0  # default
        if cross_result != 0.0:
            rel_cross_err = abs(cross_error / cross_result)
        elif cross_error != 0.0:
            raise ValueError(self._logger_prefix + f"::run:  val={cross_result}, err={cross_error}")
        min_rel_err: float = 1e-9
        if rel_cross_err < min_rel_err:
            with self.session as session:
                self._logger(
                    session,
                    self._logger_prefix
                    + f"::run:  very small relative error {rel_cross_err:.3e}, setting to min_rel_err",
                    level=LogLevel.WARN,
                )
            rel_cross_err = min_rel_err

        cross_list.append((1.0, min_rel_err))  # safe guard against all-zero case
        max_rel_hist_err: float = max(abs(e / r) for r, e in cross_list if r != 0.0)
        if opt_target == "cross":
            pass  # keep cross error for optimisation
        elif opt_target == "cross_hist":
            # rel_cross_err = (rel_cross_err+max_rel_hist_err)/2.0
            # > since we took the worst case for max_rel_hist_err, let's take a geometric mean
            rel_cross_err = math.sqrt(rel_cross_err * max_rel_hist_err)
        elif opt_target == "hist":
            rel_cross_err = max_rel_hist_err
        else:
            raise ValueError(self._logger_prefix + f"::run:  unknown opt_target {opt_target}")
        final_error: float = abs(rel_cross_err * cross_result)

        # > mark part merging as complete in the DB.  HDF5 observable timestamps
        # > represent input freshness for MergeObs and must not be advanced here:
        # > doing so makes freshly generated .dat files look stale on resume.
        ts: float = time.time()

        # > Phase 4: short DB session: persist cross-section result and completion timestamp
        with self.session as session:
            pt = session.get_one(Part, self.part_id)
            pt.result = cross_result
            pt.error = final_error
            pt.timestamp = ts
            self._debug(
                session,
                self._logger_prefix
                + f"::run: {max_rel_hist_err=}  pt.result = {pt.result} +/- {pt.error} (rel_err = {rel_cross_err:.3e})",
            )
            self._safe_commit(session)

        #############################

        if not self.force and resize_max > 1:
            # > we have to skip pre-productions to trigger `MergeAll`
            # > as it is not guaranteed that all parts exist yet
            yield self.clone(cls=MergeAll)


class MergeAll(DBMerge):
    # > merge all `Part` objects that are currently active
    finalize: bool = luigi.BoolParameter(default=False)  # type: ignore[assignment]

    priority = 110

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger_prefix: str = "MergeAll"
        if self.force or self.reset_tag > 0.0 or self.finalize:
            self._logger_prefix += (
                "["
                + ", ".join(
                    ([f"force={self.force}"] if self.force else [])
                    + ([f"reset={time.ctime(self.reset_tag)}"] if self.reset_tag > 0.0 else [])
                    + (["finalize"] if self.finalize else [])
                )
                + "]"
            )
        with self.session as session:
            self._debug(session, self._logger_prefix + "::init")
        # > output directory
        self.mrg_path: Path = self._path.joinpath("result", "merge")
        if not self.mrg_path.exists():
            self.mrg_path.mkdir(parents=True)
        self.merge_marker: Path = self._path.joinpath("result", "merge_all.json")

    @property
    def select_part(self):
        return select(Part).where(Part.active.is_(True))

    def requires(self):
        if self.force or self.reset_tag > 0.0:
            with self.session as session:
                self._debug(session, self._logger_prefix + "::requires:  return parts...")
                return [self.clone(cls=MergePart, part_id=pt.id) for pt in session.scalars(self.select_part)]
        else:
            return []

    def _read_merge_marker(self) -> dict | None:
        if not self.merge_marker.is_file():
            return None
        with self.merge_marker.open() as marker_file:
            return json.load(marker_file)

    def complete(self) -> bool:
        # > check input requirements
        if any(not mpt.complete() for mpt in self.requires()):
            return False

        if self.finalize:
            marker = self._read_merge_marker()
            if marker is None:
                return False
            if self.run_tag > float(marker.get("run_tag", -1.0)):
                return False
            return bool(marker.get("finalized", False))

        marker = self._read_merge_marker()
        if marker is None:
            return False
        marker_run_tag = float(marker.get("run_tag", -1.0))
        if self.run_tag > marker_run_tag:
            return False
        marker_part_ids = marker.get("active_part_ids")
        marker_max_part_timestamp = float(marker.get("max_part_timestamp", -1.0))
        marker_outputs = marker.get("output_observables")
        if not isinstance(marker_part_ids, list) or not isinstance(marker_outputs, list):
            return False
        if any(not (self.mrg_path / f"{obs}.dat").is_file() for obs in marker_outputs):
            return False

        with self.session as session:
            self._debug(
                session,
                self._logger_prefix
                + f"::complete:  marker {datetime.datetime.fromtimestamp(marker_run_tag)}",
            )
            active_parts: list[Part] = session.scalars(self.select_part).all()
            active_part_ids = [pt.id for pt in active_parts]
            if set(active_part_ids) != set(marker_part_ids):
                return False
            # > pt.timestamp < 0 is the "merge in progress" sentinel (set in MergePart.run phase 1);
            # > guard against a crashed MergePart leaving that state memoised by a stale marker
            if any(pt.timestamp < 0 for pt in active_parts):
                return False
            max_part_timestamp = max((pt.timestamp for pt in active_parts), default=-1.0)
            for pt in active_parts:
                self._debug(
                    session,
                    self._logger_prefix
                    + f"::complete:  {pt.name} {datetime.datetime.fromtimestamp(pt.timestamp)}",
                )
            return max_part_timestamp <= marker_max_part_timestamp

    def run(self):  # type: ignore[override]
        with self.session as session:
            self._logger(session, self._logger_prefix + "::run")
            mrg_parent: Path = self._path.joinpath("result", "part")

            # > collect all input files
            in_files = dict((obs, []) for obs in self.config["run"]["histograms"])
            active_part_ids: list[int] = []
            max_part_timestamp: float = -1.0
            # > reconstruct optimisation target
            opt_target: str = self.config["run"]["opt_target"]
            opt_target_ref: float = 0.0
            opt_target_rel: float = 0.0
            for pt in session.scalars(self.select_part):
                active_part_ids.append(pt.id)
                max_part_timestamp = max(max_part_timestamp, pt.timestamp)
                self._debug(
                    session,
                    self._logger_prefix + f"::run:  processing part {pt.name}: {pt.result} +/- {pt.error}",
                )
                opt_target_ref += pt.result
                opt_target_rel += pt.error**2
                for obs in self.config["run"]["histograms"]:
                    in_file: Path = mrg_parent / pt.name / f"{obs}.dat"
                    if in_file.exists():
                        in_files[obs].append(str(in_file.relative_to(self._path)))
                    # > if we add new histograms to template.run later, need to allow the file not to exist
                    # else:
                    #     raise FileNotFoundError(f"MergeAll::run:  missing {in_file}")
            if opt_target_ref != 0.0:
                opt_target_rel = math.sqrt(opt_target_rel) / abs(opt_target_ref)  # relative uncertainty

            # > use `distribute_time` to fetch optimization target
            # > use small 1s value; a non-zero time to avoid division by zero
            # > the above does not include penalty, which is why we override it this way
            opt_dist = self._distribute_time(session, 1.0)
            opt_target_ref = opt_dist["tot_result"]
            opt_target_rel = (
                abs(opt_dist["tot_error"] / opt_dist["tot_result"]) if opt_dist["tot_result"] != 0.0 else 0.0
            )

            # > sum all parts
            written_observables: list[str] = []
            for obs, hist_info in self.config["run"]["histograms"].items():
                out_file: Path = self.mrg_path / f"{obs}.dat"
                nx: int = hist_info["nx"]
                qwgt: bool = self.grids and _obs_has_grid(hist_info)
                if len(in_files[obs]) == 0:
                    self._logger(
                        session,
                        self._logger_prefix + f"::run:  no files for {obs}",
                        level=LogLevel.ERROR,
                    )
                    continue
                hist = NNLOJETHistogram()
                for in_file in in_files[obs]:
                    try:
                        hist = hist + NNLOJETHistogram(nx=nx, filename=self._path / in_file, weights=qwgt)
                    except ValueError as e:
                        self._logger(session, f"error reading file {in_file} ({e!r})", level=LogLevel.ERROR)
                hist.write_to_file(out_file)
                written_observables.append(obs)
                if qwgt:
                    weights_file = out_file.with_suffix(".weights.txt")
                    weights_file.write_text(hist.to_weights())
                if obs == "cross":
                    with open(out_file) as cross:
                        for line in cross:
                            if line.startswith("#"):
                                continue
                            col: list[float] = [float(c) for c in line.split()]
                            res: float = col[0]
                            # err: float = col[1]
                            # rel: float = abs(err / res) if res != 0.0 else float("inf")
                            self._logger(
                                session,
                                # f"[blue]cross = ({res} +/- {err}) fb  \[{rel * 1e2:.3}%][/blue]\n"
                                f"[blue]cross = {res} fb[/blue]\n"
                                + f'[magenta][dim]current "{opt_target}" error:[/dim]\n'
                                + f"{opt_target_rel * 1e2:.3}% (requested: {self.config['run']['target_rel_acc'] * 1e2:.3}%)[/magenta]",
                                level=LogLevel.SIG_UPDXS,
                            )
                            break
        marker = {
            "run_tag": self.run_tag,
            "active_part_ids": active_part_ids,
            "n_active_parts": len(active_part_ids),
            "max_part_timestamp": max_part_timestamp,
            "output_observables": written_observables,
            "generated_at": time.time(),
        }
        marker_tmp = self.merge_marker.with_suffix(".json.tmp")
        with marker_tmp.open("w") as marker_file:
            json.dump(marker, marker_file, indent=2, sort_keys=True)
            marker_file.write("\n")
        marker_tmp.replace(self.merge_marker)

        if self.finalize:
            fin_path: Path = self._path.joinpath("result", "final")
            if not fin_path.exists():
                fin_path.mkdir(parents=True)
            mrg_parent_fin: Path = self._path.joinpath("result", "part")
            with self.session as session:
                for out_order in Order:
                    select_order = select(Part)  # no need to be active: .where(Part.active.is_(True))
                    if int(out_order) < 0:
                        select_order = select_order.where(Part.order == out_order)
                    else:
                        select_order = select_order.where(func.abs(Part.order) <= out_order)
                    matched_parts = session.scalars(select_order).all()

                    # > is there even a Part at this order for this process? (NNLO for an NLO-only process)
                    if not session.scalars(
                        select(Part).where(func.abs(Part.order) == abs(out_order))
                    ).first():
                        self._logger(session, self._logger_prefix + f"::run:  no parts at order {out_order}")
                        continue

                    # > in order to write out an `order` result, we need at least one complete result for each part
                    if any(pt.ntot <= 0 for pt in matched_parts):
                        self._logger(
                            session,
                            f'[red]{self._logger_prefix}::run:  skipping "{out_order}" due to incomplete parts[/red]',
                        )
                        continue

                    self._debug(
                        session,
                        self._logger_prefix
                        + f"::run:  {out_order}: {list(map(lambda x: (x.id, x.ntot), matched_parts))}",
                    )

                    in_files_fin = dict((obs, []) for obs in self.config["run"]["histograms"])
                    for pt in matched_parts:
                        for obs in self.config["run"]["histograms"]:
                            in_file: Path = mrg_parent_fin / pt.name / f"{obs}.dat"
                            if in_file.exists():
                                in_files_fin[obs].append(str(in_file.relative_to(self._path)))
                            else:
                                # > can happen when new histogram added manually
                                self._logger(
                                    session,
                                    self._logger_prefix + f"::run:  skipping missing file: {in_file}",
                                    level=LogLevel.WARN,
                                )

                    # > sum all parts
                    for obs, hist_info in self.config["run"]["histograms"].items():
                        out_file: Path = fin_path / f"{out_order}.{obs}.dat"
                        nx: int = hist_info["nx"]
                        qwgt: bool = self.grids and _obs_has_grid(hist_info)
                        if len(in_files_fin[obs]) == 0:
                            self._logger(
                                session,
                                self._logger_prefix + f"::run:  no files for {obs}",
                                level=LogLevel.ERROR,
                            )
                            continue
                        hist = NNLOJETHistogram()
                        for in_file in in_files_fin[obs]:
                            try:
                                hist = hist + NNLOJETHistogram(
                                    nx=nx, filename=self._path / in_file, weights=qwgt
                                )
                            except ValueError as e:
                                self._logger(
                                    session,
                                    self._logger_prefix + f"::run:  error reading file {in_file} ({e!r})",
                                    level=LogLevel.ERROR,
                                )
                        hist.write_to_file(out_file)
                        if qwgt:
                            weights_file = out_file.with_suffix(".weights.txt")
                            weights_file.write_text(hist.to_weights())
                            pine_merge: Path = (
                                Path(self.config["exe"]["path"]).parent / "nnlojet-merge-pineappl"
                            )
                            if pine_merge.is_file() and os.access(pine_merge, os.X_OK):
                                job_env = os.environ.copy()
                                # > all parts ready -> combine into final grid
                                grid_file: Path = out_file.with_suffix(".pineappl.lz4")
                                grid_log: Path = out_file.with_suffix(".pineappl.log")
                                with open(grid_log, "w") as log:
                                    _ = subprocess.run(
                                        [
                                            pine_merge,
                                            str(weights_file.relative_to(out_file.parent)),
                                            str(grid_file.relative_to(out_file.parent)),
                                            "-v",
                                            "--skip",
                                            "--noopt",
                                        ],
                                        env=job_env,
                                        cwd=out_file.parent,
                                        stdout=log,
                                        stderr=log,
                                        text=True,
                                    )
                            else:
                                self._logger(
                                    session,
                                    f"[red]{self._logger_prefix}::run:  missing nnlojet-merge-pineappl executable at {pine_merge}[/red]",
                                    level=LogLevel.ERROR,
                                )
            # > re-write marker atomically with finalized flag added
            marker["finalized"] = True
            marker_tmp = self.merge_marker.with_suffix(".json.tmp")
            with marker_tmp.open("w") as marker_file:
                json.dump(marker, marker_file, indent=2, sort_keys=True)
                marker_file.write("\n")
            marker_tmp.replace(self.merge_marker)


class MergeFinal(DBMerge):
    # > a final merge of all orders where we have parts available

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger_prefix: str = "MergeFinal"
        with self.session as session:
            if self.force or self.reset_tag > 0.0:
                self._logger_prefix = (
                    self._logger_prefix + f"[force={self.force}, reset={time.ctime(self.reset_tag)}]"
                )
            self._debug(session, self._logger_prefix + "::init")

        # > output directory
        self.fin_path: Path = self._path.joinpath("result", "final")

        self.result = float("nan")
        self.error = float("inf")

    def requires(self):
        with self.session as session:
            self._debug(session, self._logger_prefix + "::requires")
        return [self.clone(MergeAll, force=True, finalize=True)]

    def complete(self) -> bool:
        with self.session as session:
            self._debug(session, self._logger_prefix + "::complete")
            last_sig = session.scalars(select(Log).where(Log.level < 0).order_by(Log.id.desc())).first()
            self._debug(session, self._logger_prefix + f"::complete:  last_sig = {last_sig!r}")
            if last_sig and last_sig.level in [LogLevel.SIG_COMP]:
                return True
        return False

    def run(self):  # type: ignore[override]
        with self.session as session:
            self._logger(session, self._logger_prefix + "::run")

            # > shut down the monitor
            self._logger(session, "complete", level=LogLevel.SIG_COMP)
            time.sleep(self.config["ui"]["refresh_delay"])

            # > parse merged cross section result
            mrg_all: MergeAll = self.requires()[0]
            dat_cross: Path = mrg_all.mrg_path / "cross.dat"
            with open(dat_cross) as cross:
                for line in cross:
                    if line.startswith("#"):
                        continue
                    self.result = float(line.split()[0])
                    self.error = float(line.split()[1])
                    break
            rel_acc: float = abs(self.error / self.result)
            # > compute total runtime invested
            T_tot: float = sum(pt.Ttot for pt in session.scalars(select(Part).where(Part.active.is_(True))))
            self._logger(
                session,
                f"\n[blue]cross = ({self.result} +/- {self.error}) fb  [{rel_acc * 1e2:.3}%][/blue]"
                + f"\n[dim](total runtime invested: {format_time_interval(T_tot)})[/dim]",
            )
            # > use `distribute_time` to fetch optimization target
            # > & time estimate to reach desired accuracy
            # > use small 1s value; a non-zero time to avoid division by zero
            prev_T_target: float = 1.0
            opt_dist = self._distribute_time(session, prev_T_target)
            # self._logger(session,f"{opt_dist}")
            opt_target: str = self.config["run"]["opt_target"]
            self._logger(
                session,
                f'option "[bold]{opt_target}[/bold]" chosen to target optimization of rel. acc.',
            )
            rel_acc: float = abs(opt_dist["tot_error"] / opt_dist["tot_result"])
            if rel_acc <= self.config["run"]["target_rel_acc"] * (1.05):
                self._logger(
                    session,
                    f"[green]reached rel. acc. {rel_acc * 1e2:.3}% on {opt_target}[/green] "
                    + f"(requested: {self.config['run']['target_rel_acc'] * 1e2:.3}%)",
                )
            else:
                self._logger(
                    session,
                    f"[red]reached rel. acc. {rel_acc * 1e2:.3}% on {opt_target}[/red] "
                    + f"(requested: {self.config['run']['target_rel_acc'] * 1e2:.3}%)",
                )
                T_target: float = opt_dist["T_target"]
                # > because of inequality constraints, need to loop to find reliable estimate
                while T_target / prev_T_target > 1.3:
                    opt_dist = self._distribute_time(session, T_target)
                    prev_T_target = T_target
                    T_target = opt_dist["T_target"]
                njobs_target: int = sum(ires["njobs"] for _, ires in opt_dist["part"].items())
                self._logger(
                    session,
                    "still require about "
                    + f"[bold]{format_time_interval(T_target)}[/bold]"
                    + " of runtime to reach desired target accuracy"
                    + f" [dim](approx. {njobs_target} jobs)[/dim]",
                )
