"""dokan merge tasks

defines tasks to merge individual NNLOJET results into a combined result.
constitutes the dokan workflow implementation of `nnlojet-combine.py`
"""

import datetime
from enum import IntEnum, unique
import math
import os
import re
import shutil
import subprocess
import time
from abc import ABCMeta
from pathlib import Path

import h5py
import luigi
import numpy as np
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .._types import GenericPath
from ..combine import NNLOJETContainer, NNLOJETHistogram
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
_dt_hist = np.dtype([("neval", np.int64), ("result", np.float64), ("error2", np.float64)], align=True)


@unique
class BinMask(IntEnum):
    """possible values for the bin mask"""

    ACTIVE = 0
    TRIMMED = 1
    INVALID = 2


class DBMerge(DBTask, metaclass=ABCMeta):
    # > flag to force a re-merge (if new jobs are in a `done` state but not yet `merged`)
    force: bool = luigi.BoolParameter(default=False)
    # > tag to trigger a reset to initiate a re-merge from scratch (timestamp)
    reset_tag: float = luigi.FloatParameter(default=-1.0)
    # > flag to trigger write-out of weights for interpolation grids
    grids: bool = luigi.BoolParameter(default=False)

    priority = 120

    # > limit the resources on local cores
    @property
    def resources(self):
        return super().resources | {"local_ncores": 1}

    def _make_prefix(self, session: Session | None = None) -> str:
        return self.__class__.__name__ + "[" + ", ".join(([f"force={self.force}"] if self.force else []) + ([f"reset={time.ctime(self.reset_tag)}"] if self.reset_tag > 0.0 else [])) + "]"


class MergeObs(Task):
    hdf5_in: GenericPath = luigi.Parameter()
    hdf5_path: list[str] = luigi.ListParameter()  # path to the observable group
    dat_out: GenericPath = luigi.Parameter()

    priority = 130

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_hdf5: Path = self._path / self.hdf5_in
        self.file_dat: Path = self._path / self.dat_out
        if not self.file_hdf5.is_file():
            raise FileNotFoundError(f"MergeObs:  HDF5 input file {self.file_hdf5} does not exist!")
        with h5py.File(self.file_hdf5, "r", libver="latest", swmr=True) as h5f:
            h5grp_obs: h5py.Group = h5f["/".join(self.hdf5_path)]
            self.timestamp: float = h5grp_obs.attrs.get("timestamp", 0)

    # > limit the resources on local cores
    @property
    def resources(self):
        # return super().resources | {"local_ncores": 1, "MergeObs": 1}
        return super().resources | {"local_ncores": 1}

    def complete(self):
        if self.file_dat.is_file():
            return self.file_dat.stat().st_mtime >= self.timestamp
        else:
            return False

    def run(self):
        print(f"MergeObs:  {self.hdf5_in}{self.hdf5_path} > {self.dat_out}")
        trim_threshold: float = self.config["merge"]["trim_threshold"]
        trim_max_fraction: float = self.config["merge"]["trim_max_fraction"]
        k_scan_nsteps: int = self.config["merge"]["k_scan_nsteps"]
        k_scan_maxdev_steps: float = self.config["merge"]["k_scan_maxdev_steps"]
        with h5py.File(self.file_hdf5, "r", libver="latest", swmr=True) as h5f:
            h5grp_obs: h5py.Group = h5f["/".join(self.hdf5_path)]
            nx: int = h5grp_obs.attrs["nx"]
            h5dat_data: h5py.Dataset = h5grp_obs["data"]
            nrows, ncols, ndat = h5dat_data.shape
            # > define arrays to avoid re-allocation
            bin_data = np.empty((ndat,), dtype=_dt_hist)
            bin_sumf = np.empty((ndat + 1,), dtype=_dt_hist)  # one trailing entry to accumulate "trimmed" datasets
            bin_mask = np.empty((ndat + 1,), dtype=np.int32)  # mask to keep track of trimmed data (0: active, 1: trimmed, 2: invalid, <0: merged)
            # > buffers for intermediate operations
            bin_buf1 = np.empty((ndat + 1,), dtype=np.float64)
            bin_buf2 = np.empty((ndat + 1,), dtype=np.float64)
            # > the final merged result
            merged_hist = np.zeros((nrows, ncols), dtype=_dt_hist)
            weights = np.full((nrows, ndat), np.nan, dtype=np.float64)  ### if self.grids else None

            # > more information needed for the output
            if nx > 0:
                xval = h5dat_data.dims[0][0][...]
            else:
                xval = None
            labels = h5grp_obs.attrs.get("labels", None)

            def combine_unweighted() -> tuple[np.float64, np.float64]:
                # > unweigthed average as a reference
                nonlocal bin_sumf, bin_mask
                _mask = bin_mask == BinMask.ACTIVE
                _neval = np.sum(bin_sumf["neval"], where=_mask)
                _result = np.sum(bin_sumf["result"], where=_mask) / _neval
                _error = np.sqrt(np.sum(bin_sumf["error2"], where=_mask) - _result**2 * _neval) / _neval
                return _result, _error

            def combine_weighted() -> tuple[np.float64, np.float64]:
                # > compute the weighted average using the sumf arrays
                nonlocal bin_sumf, bin_mask
                nonlocal bin_buf1, bin_buf2
                _mask = (bin_mask == BinMask.ACTIVE) & (bin_sumf["error2"] > 0.0)
                bin_buf1[:] = 0
                bin_buf2[:] = 0
                np.multiply(bin_sumf["result"], bin_sumf["result"], out=bin_buf1, where=_mask)
                np.divide(bin_buf1, bin_sumf["neval"], out=bin_buf1, where=_mask)
                np.subtract(bin_sumf["error2"], bin_buf1, out=bin_buf1, where=_mask)
                np.multiply(bin_sumf["neval"], bin_sumf["neval"], out=bin_buf2, where=_mask)
                np.divide(bin_buf1, bin_buf2, out=bin_buf1, where=_mask)
                np.reciprocal(bin_buf1, out=bin_buf1, where=_mask)
                _error = np.sum(bin_buf1[_mask])
                if _error > 0.0:
                    np.divide(bin_sumf["result"], bin_sumf["neval"], out=bin_buf2, where=_mask)
                    np.multiply(bin_buf2, bin_buf1, out=bin_buf2, where=_mask)
                    _result = np.sum(bin_buf2, where=_mask) / _error
                    _error = 1.0 / np.sqrt(_error)
                else:
                    _result = np.float64(0.0)
                    _error = np.float64(0.0)
                return _result, _error

            def merge_pair() -> None:
                # > small & large stats to average out differences in (pseudo-)job statistics
                nonlocal bin_sumf, bin_mask
                _ibuf = np.argsort(bin_sumf["neval"])
                _mask = bin_mask == BinMask.ACTIVE
                nstart = np.sum(_mask)
                # print(f"{bin_sumf["neval"]=}")
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
                    bin_sumf["neval"][upp] += bin_sumf["neval"][low]
                    bin_sumf["result"][upp] += bin_sumf["result"][low]
                    bin_sumf["error2"][upp] += bin_sumf["error2"][low]
                    bin_sumf[low] = 0  # reset
                    bin_mask[low] = -upp
                    _mask[low] = False
                    # print(f"  >  merged {low} into {upp}")
                    # > move to next pair
                    ilow += 1
                    iupp -= 1
                    low = _ibuf[ilow]
                    upp = _ibuf[iupp]
                nend = np.sum(_mask)
                assert nend < nstart

            for irow in range(nrows):
                for icol in range(ncols):
                    # > populate the arrays to perform the merge
                    h5dat_data.read_direct(bin_data, source_sel=np.s_[irow, icol, :])
                    # > we operate on the f & f2 cumulants from here on, leave `bin_data` alone
                    bin_sumf[:] = 0
                    bin_sumf["neval"][:ndat] = bin_data["neval"]
                    # X  bin_sumf["result"][:ndat] = bin_data["neval"] * bin_data["result"]
                    np.multiply(bin_data["neval"], bin_data["result"], out=bin_sumf["result"][:ndat])
                    # X  bin_sumf["error2"][:ndat] = bin_data["neval"] ** 2 * bin_data["error2"] + bin_data["neval"] * bin_data["result"] ** 2
                    bin_buf1[:] = 0
                    bin_buf2[:] = 0
                    np.multiply(bin_data["neval"], bin_data["neval"], out=bin_buf1[:ndat])
                    np.multiply(bin_data["error2"], bin_buf1[:ndat], out=bin_buf1[:ndat])
                    np.multiply(bin_data["result"], bin_data["result"], out=bin_buf2[:ndat])
                    np.multiply(bin_data["neval"], bin_buf2[:ndat], out=bin_buf2[:ndat])
                    np.add(bin_buf1[:ndat], bin_buf2[:ndat], out=bin_sumf["error2"][:ndat])

                    # > some cleanup & flagging of invalid entries
                    bin_mask[:] = BinMask.ACTIVE  # switch on all entries
                    bin_mask[ndat] = BinMask.INVALID  # "trimmed" entry not yet populated
                    bin_mask[:ndat][~np.isfinite(bin_data["result"])] = BinMask.INVALID  # discard all non-finite results (nan, +/- inf)
                    bin_mask[:ndat][bin_data["neval"] <= 0] = BinMask.INVALID  # discard all entries with zero evaluations
                    bin_sumf[bin_mask == BinMask.INVALID] = 0
                    # > error = zero should only happen if result is also zero
                    assert np.all(bin_data["result"][bin_data["error2"] == 0.0] == 0.0)

                    # > appy outlier trimming
                    # > we'll use MAD instead of IQR as it is easier to convert to a standard z-score
                    _mask = (bin_mask == BinMask.ACTIVE) & (bin_sumf["error2"] > 0.0)  # exclude "zero bins" from being trimmed
                    if trim_threshold > 0.0 and np.sum(_mask) > 1:
                        q25, q50, q75 = np.quantile(bin_data["result"][_mask[:ndat]], [0.25, 0.50, 0.75])
                        bin_buf1[:] = 0
                        bin_buf1[_mask] = np.abs(bin_data["result"][_mask[:ndat]] - q50)  # `ndat` entry invalid: no need for [:ndat] on lhs
                        mad = np.median(bin_buf1[_mask])
                        threshold = trim_threshold * (mad / 0.6745)  # convert to z-score
                        # > start trimming from the "worst" until we either run out or would exceed the max fraction
                        # > skip `[_mask]` since initialised to zero (makes indexing easier than for sliced arrays)
                        # X  bin_mask[bin_buf1 > threshold] = BinMask.TRIMMED
                        avg_neval = np.sum(bin_sumf["neval"][_mask]) / (1.0 + np.sum(_mask))
                        ntrim: int = 0
                        for itrim in np.argsort(bin_buf1)[::-1]:
                            if bin_buf1[itrim] <= threshold:
                                break
                            if (ntrim + 1) > trim_max_fraction * ndat:
                                break
                            # > we correct for the fact that the data samples can be based on different statistics
                            if bin_buf1[itrim] > threshold * np.sqrt(avg_neval / bin_sumf["neval"][itrim]):
                                bin_mask[itrim] = BinMask.TRIMMED
                                ntrim += 1
                                # print(f" > trim {irow},{icol} [{itrim}] {bin_buf1[itrim]:.3f} > {threshold * np.sqrt(avg_neval / bin_sumf['neval'][itrim]):.3f} ({ntrim}/{ndat})")
                        # > we will not discard the trimmed datasets but actually accumulate them into a mega "outlier" dataset
                        # > which will eventually be suppressed in the weighted average by the large error
                        _mask = bin_mask == BinMask.TRIMMED
                        bin_sumf["neval"][ndat] = np.sum(bin_sumf["neval"][_mask])
                        bin_sumf["result"][ndat] = np.sum(bin_sumf["result"][_mask])
                        bin_sumf["error2"][ndat] = np.sum(bin_sumf["error2"][_mask])
                        bin_sumf[_mask] = 0
                        bin_mask[ndat] = BinMask.ACTIVE if bin_sumf["neval"][ndat] > 0 else BinMask.INVALID
                        # if bin_mask[ndat] == BinMask.ACTIVE:
                        #     print(f"trimmed {bin_sumf['neval'][ndat]} [{irow},{icol}]")
                        bin_mask[ndat] = BinMask.INVALID  # keep it trimmed for now

                    # print(f"\n### {self.hdf5_path[0]}__{self.hdf5_path[1]}__{irow}__{icol}  active = {np.sum(bin_mask == BinMask.ACTIVE)}, non-zero = {np.sum(bin_sumf['error2'] > 0.0)}")

                    # > weighted average cannot deal with "zero bins" but those jobs still matter and should not be discarded
                    # > do a pairwise merge of (pseudo-)jobs until there are no "zero bins" or there's only one psuedo-job left
                    while True:
                        _mask = bin_mask == BinMask.ACTIVE
                        if np.sum(_mask) <= 1 or np.sum(bin_sumf["error2"][_mask] == 0.0) <= 0:
                            break
                        # print(f"  > merge {np.sum(bin_mask == BinMask.ACTIVE)} active bins, {np.sum(bin_sumf['error2'] > 0.0)} non-zero bins")
                        merge_pair()

                    # > perform the k-scan
                    _neval = sum(bin_sumf["neval"])  # no mask(!) since err=0 events also count
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

                    merged_hist[irow, icol] = (_neval, *k_scan[-1][:2])
                    # > determine weights (only for the "central" prediction)
                    if icol == 0:
                        for idat in range(ndat):
                            if (bin_mask[idat] == BinMask.INVALID) or (bin_mask[idat] == BinMask.TRIMMED):
                                weights[irow, idat] = 0.0
                            elif bin_mask[idat] == BinMask.ACTIVE:
                                # > the merged/absorbed data will be set in this "parent" active case
                                # > the weight from the weighted average
                                _neval, _sumf, _sumf2 = bin_sumf[idat]
                                _ierr2 = (_sumf2 - _sumf**2 / _neval) / _neval**2
                                if _ierr2 <= 0.0:
                                    assert merged_hist[irow, icol]["error2"] == 0.0
                                    _iwgt = 0.0
                                else:
                                    _iwgt = (1.0 / _ierr2) * merged_hist[irow, icol]["error2"] ** 2
                                # > find all "nodes" that were merged into `idat`
                                _inode: int = 0
                                _nodes = np.array([idat])
                                while _inode < len(_nodes):
                                    # > find all children of the current node
                                    # print(f"  | {_inode}:{_nodes[_inode]} | {_nodes} | {np.flatnonzero(bin_mask == -_nodes[_inode])}")
                                    if _nodes[_inode] != 0:
                                        _nodes = np.concatenate((_nodes, np.flatnonzero(bin_mask == -_nodes[_inode])))
                                    _inode += 1
                                # print(f" > nodes[{idat}]: {_nodes}")
                                for _inode in _nodes:
                                    weights[irow, _inode] = _iwgt * bin_data["neval"][_inode] / _neval
                        # print(f" > weights: {weights[irow,:]}")
                        # print(f" > sum of weights [{irow}] = {np.sum(weights[irow, :]):.3f}")
                        assert np.all(np.isfinite(weights[irow, :]))  # check that we have weights for all entries
                        if not np.all(np.isfinite(weights[irow, :])):
                            print(f" > mask: {bin_mask}")
                            time.sleep(10)

        with open(self.file_dat, "w") as df:
            if labels is not None:
                df.write(labels + "\n")
            df.write(f"#neval: {np.max(merged_hist['neval'])}\n")
            for irow in range(nrows):
                if xval is not None:
                    if np.all(np.isnan(xval[irow])):
                        if nx == 3:
                            df.write("#overflow:lower center upper ")
                        else:
                            df.write("#overflow: ")
                    else:
                        for x in xval[irow]:
                            df.write(f"{np.format_float_scientific(x): <23} ")
                for icol in range(ncols):
                    df.write(np.format_float_scientific(merged_hist["result"][irow, icol]) + " ")
                    df.write(np.format_float_scientific(merged_hist["error2"][irow, icol]) + " ")
                df.write("\n")


class MergePart(DBMerge):
    # > merge only a specific `Part`
    part_id: int = luigi.IntParameter()

    # > limit the resources on local cores
    @property
    def resources(self):
        return super().resources | {"local_ncores": 1, f"MergePart_{self.part_id}": 1}

    # @property
    # def select_part(self):
    #     return select(Part).where(Part.id == self.part_id).where(Part.active.is_(True))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger_prefix: str = "MergePart"
        with self.session as session:
            pt: Part = session.get_one(Part, self.part_id)
            self._logger_prefix = (
                self._logger_prefix + f"[{pt.name}" + (f", force={self.force}" if self.force else "") + (f", reset={time.ctime(self.reset_tag)}" if self.reset_tag > 0.0 else "") + "]"
            )
            self._debug(session, self._logger_prefix + "::init")

    @property
    def select_job(self):
        return (
            select(Job).join(Part).where(Part.id == self.part_id).where(Part.active.is_(True)).where(Job.mode == ExecutionMode.PRODUCTION).where(Job.status.in_(JobStatus.success_list()))
            # @todo: why did I have this? -> ".where(Job.timestamp < Part.timestamp)"
        )

    def complete(self) -> bool:
        with self.session as session:
            pt: Part = session.get_one(Part, self.part_id)

            query_job = (
                session.query(Job)
                .join(Part)
                .filter(Part.id == self.part_id)
                .filter(Part.active.is_(True))
                .filter(Job.mode == ExecutionMode.PRODUCTION)
                .filter(Job.status.in_(JobStatus.success_list()))
            )

            c_done = query_job.filter(Job.status == JobStatus.DONE).count()
            c_merged = query_job.filter(Job.status == JobStatus.MERGED).count()

            if (c_done + c_merged) == 0:
                self._debug(
                    session,
                    self._logger_prefix + f"::complete:  #done={c_done}, #merged={c_merged} => mark complete",
                )
                # @todo raise error as we should never be in this situation?
                return True

            self._debug(
                session,
                self._logger_prefix + f"::complete:  #done={c_done}, #merged={c_merged}, timestamp={time.ctime(pt.timestamp)}",
            )

            if self.force and c_done > 0:
                return False

            if pt.timestamp < self.reset_tag:
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
            if self.config["production"]["min_number"] > 0 and c_done > 0 and c_merged <= self.config["production"]["min_number"]:
                return False

            if float(c_done + c_merged + 1) / float(c_merged + 1) < self.config["production"]["fac_merge_trigger"]:
                return True

            self._debug(
                session,
                self._logger_prefix + f"::complete:  #done={c_done}, #merged={c_merged} => time for a re-merge",
            )

        return False

    def run(self):
        if self.complete():
            with self.session as session:
                self._debug(
                    session,
                    self._logger_prefix + "::run:  already complete",
                )
            return

        with self.session as session:
            # > get the part and update timestamp to tag for 'MERGE'
            pt: Part = session.get_one(Part, self.part_id)
            self._logger(session, self._logger_prefix + "::run")

            # > output directory
            mrg_path: Path = self._path.joinpath("result", "part", pt.name)
            if not mrg_path.exists():
                mrg_path.mkdir(parents=True)

            # > raw data path: need to move output files if not already moved
            if (raw_path := self.config["run"].get("raw_path")) is not None:
                raw_path = Path(raw_path)

            # > populate a dictionary with all histogram files (reduces IO)
            in_files: dict[str, list[GenericPath]] = dict()
            single_file: str | None = self.config["run"].get("histograms_single_file", None)
            if single_file:
                in_files[single_file] = []  # all hist in single file
            else:
                in_files = dict((obs, []) for obs in self.config["run"]["histograms"].keys())
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
                                self._logger_prefix + "::run:  " + f"unmatched observable {dat.group(1)}?! ({in_files.keys()})",
                            )
                job.status = JobStatus.MERGED

            # > commit merged status and timestamp before yielding
            pt.timestamp = time.time()
            self._safe_commit(session)

            #############################
            # > create HDF5 file
            # @todo: actually append data and not re-write it from scratch each time
            # @todo: add a mask? -> no! MergeObs should only read
            # @todo refactor into separate member routine?
            # @todo: add move to `raw_path`
            # @todo: add compression (?works for vlen?); alternatively use fixed-size arrays with a copy-to-larger-shape-delete-original-rename workflow?
            # @todo: better to save sumf & sumf2? -> not so convenient for outliers and weighted avg but very convenient for unweighted combination and this also for the k-scan algorithm.
            # could start by storing res & err, then switch to sumf & sumf2 later when we want to apply the k-scan?
            # maybe an attribute to flag what of the two is stored in the datase? heler routine to convert between the two could also be nice.
            # with h5py.File(self._path / "raw" / f"{pt.name}.hdf5", "w", libver="latest") as h5f:
            with h5py.File(self._path / "raw" / f"{pt.name}.hdf5", "a", libver="latest") as h5f:
                # with h5py.File(self._path / "raw" / f"{pt.name}.hdf5", "a", libver="latest") as h5f:
                # > "single writer multiple reader" (SWMR) mode on for parallel reads in later processing stages
                h5f.swmr_mode = True
                # > retrieve top-level group; init group structure & data if needed
                if pt.name not in h5f:
                    # > new hdf5 file: initialize the group structure & attributes
                    h5grp_pt: h5py.Group = h5f.create_group(pt.name)
                    for obs, hist in self.config["run"]["histograms"].items():
                        h5grp_obs = h5grp_pt.create_group(f"{obs}")
                        h5grp_obs.attrs.create("timestamp", 0, dtype=np.float64)
                        h5grp_obs.attrs.create("nx", hist["nx"], dtype=np.int32)
                        if "cumulant" in hist:
                            h5grp_obs.attrs.create("cumulant", hist["cumulant"], dtype=np.int32)
                        if "grid" in hist:
                            h5grp_obs.attrs.create("grid", hist["grid"], dtype=_dt_vstr)
                    # > take first dat file to initialize (empty) data structures
                    if single_file:
                        # > single file
                        with open(self._path / in_files[single_file][0], "rt") as dat_file:
                            obs: str = ""
                            h5grp_obs = None
                            nx: int = 0
                            ncols: int = 0
                            nrows: int = 0
                            for line in dat_file:
                                line = line.strip()
                                if not line:
                                    continue  # skip empty lines
                                if line.startswith("#"):
                                    if line.startswith("#name"):
                                        # > write last (what about the very last obs?)
                                        # @todo move this one to a member function
                                        if h5grp_obs is not None:
                                            # > create empty datasets for this observable
                                            _ = h5grp_obs.create_dataset("files", (0,), dtype=_dt_vstr, maxshape=(None,))
                                            _ = h5grp_obs.create_dataset(
                                                "data",
                                                (nrows, ncols, 0),
                                                dtype=_dt_hist,
                                                maxshape=(nrows, ncols, None),
                                            )
                                            if nx > 0:
                                                h5dat_xval = h5grp_obs.create_dataset("xval", (nrows, nx), dtype=np.float64)
                                                # @todo: set up as dimensionscales
                                                # @todo: do not forget about the overflow bin (fix some convention where to put it <-> same as for the data storage)
                                                # @todo: h5dat_xval needs to be populatd with the bin edges (store above?)

                                        obs = line.split()[-1]
                                        h5grp_obs = h5grp_pt[obs]
                                        nx: int = h5grp_obs.attrs["nx"]
                                        ncols: int = 0
                                        nrows: int = 0
                                    elif line.startswith("#overflow"):
                                        nrows += 1
                                    elif line.startswith("#nx"):
                                        assert int(line.split()[-1]) == h5grp_obs.attrs["nx"]
                                else:
                                    nrows += 1
                                    ncols_: int = len(line.split()) - int(h5grp_obs.attrs["nx"])
                                    assert ncols_ % 2 == 0
                                    ncols_ = ncols_ // 2
                                    if ncols == 0:
                                        ncols = ncols_
                                    else:
                                        assert ncols == ncols_

                    else:
                        # > separate files for each observable
                        for obs in in_files.keys():
                            h5grp_obs: h5py.Group = h5grp_pt[obs]
                            nx: int = h5grp_obs.attrs["nx"]
                            xval: list[list[np.float64]] = []
                            ncols: int = 0
                            nrows: int = 0
                            with open(self._path / in_files[obs][0], "rt") as dat_file:
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
                            _ = h5grp_obs.create_dataset("file", (0,), dtype=_dt_vstr, maxshape=(None,))
                            h5dat_data = h5grp_obs.create_dataset(
                                "data",
                                (nrows, ncols, 0),
                                dtype=_dt_hist,
                                maxshape=(nrows, ncols, None),
                                # chunks=(nrows, ncols, 2),  # this is bad for many reads
                                chunks=(1, 1, 42),  # aims for something slightly smaller than 1024 Bytes
                                # compression="gzip",
                                compression="lzf",  # faster (de-)compressions, only for h5py
                            )
                            # print(f"new data {(nrows, ncols, 0)}: chunks = {h5dat_data.chunks}")
                            if nx > 0:
                                h5dat_xval = h5grp_obs.create_dataset("xval", (nrows, nx), dtype=np.float64, data=xval)
                                h5dat_xval.make_scale("x value")
                                h5dat_data.dims[0].attach_scale(h5dat_xval)

                    # > write to the HDF5 file (not needed)
                    # h5f.flush()

                # > all structures exist at this point
                h5grp_pt: h5py.Group = h5f[pt.name]

                # > time to populate new data
                # > _dt_hist = np.dtype([("neval", np.int64), ("result", np.float64), ("error2", np.float64)])
                # > only the case of seaparate dat files for the moment
                # @todo: add the single-file case some other time
                for obs in in_files.keys():
                    h5grp_obs: h5py.Group = h5grp_pt[obs]
                    h5dat_file: h5py.Dataset = h5grp_obs["file"]
                    h5dat_data: h5py.Dataset = h5grp_obs["data"]
                    nrows, ncols, ndat_old = h5dat_data.shape
                    # in_files_old: list[GenericPath] = [file_path.decode("utf-8") for file_path in h5dat_file]
                    in_files_old: list[GenericPath] = [file_path for file_path in h5dat_file.asstr()[:]]
                    in_files_new: list[GenericPath] = [file_path for file_path in in_files[obs] if file_path not in in_files_old]
                    ndat_new: int = len(in_files_new)
                    if ndat_new == 0:
                        # print(f"{pt.name}[{obs}]: nothing to append ({ndat_new}/{len(in_files_old)})")
                        continue
                    else:
                        # print(f"{pt.name}[{obs}]: append {in_files_new} // {in_files_old}")
                        h5grp_obs.attrs["timestamp"] = time.time()
                    assert ndat_old == len(in_files_old)
                    nx: int = h5grp_obs.attrs["nx"]
                    if nx > 0:
                        xval = h5dat_data.dims[0][0][...]
                    # > resize data structures to accommodate the new input files
                    h5dat_file.resize((ndat_old + ndat_new,))
                    h5dat_data.resize((nrows, ncols, ndat_old + ndat_new))
                    # > open each file and accummulate into the enlarged dataset
                    for idat, ifile in enumerate(in_files_new, start=ndat_old):
                        # > register the file to the "old" list
                        h5dat_file[idat] = ifile
                        with open(self._path / ifile, "rt") as dat_file:
                            irow: int = 0
                            neval: int = -1
                            for line in dat_file:
                                line = line.strip()
                                if not line:
                                    continue  # skip empty lines
                                if line.startswith("#"):
                                    if line.startswith("#overflow"):
                                        # > parse the columns fof the line, discard nx columns
                                        arr_f64 = np.fromstring(line.split(None, nx)[nx], dtype=np.float64, sep=" ")
                                        # > slice into the row, update values
                                        h5dat_data["result", irow, :, idat] = arr_f64[0::2]
                                        h5dat_data["error2", irow, :, idat] = arr_f64[1::2] ** 2
                                        irow += 1
                                    elif line.startswith("#nx"):
                                        assert int(line.split()[-1]) == nx
                                    elif line.startswith("#neval"):
                                        neval = int(line.split()[-1])
                                        h5dat_data["neval", :, :, idat] = neval
                                else:
                                    arr_f64 = np.fromstring(line, dtype=np.float64, sep=" ")
                                    if nx > 0:
                                        assert np.all(arr_f64[:nx] == xval[irow])
                                    assert len(arr_f64) - nx == 2 * ncols
                                    # > slice into the row, update values
                                    h5dat_data["result", irow, :, idat] = arr_f64[nx::2]
                                    h5dat_data["error2", irow, :, idat] = arr_f64[nx + 1 :: 2] ** 2
                                    irow += 1

            # > dispatch HDF5 file to MergeObs for each observable separately
            mrg_obs_list = yield [
                self.clone(
                    cls=MergeObs,
                    hdf5_in=str((self._path / "raw" / f"{pt.name}.hdf5").relative_to(self._path)),
                    hdf5_path=[f"{pt.name}", f"{obs}"],
                    dat_out=str((mrg_path / f"{obs}.dat2").relative_to(self._path)),
                )
                for obs in self.config["run"]["histograms"]
            ]
            # print(f"{mrg_obs_list!r}")
            # for i, mrg_obs in enumerate(mrg_obs_list, start=1):
            #     print(f"{i}: {mrg_obs}")

            #############################

            self._logger(session, self._logger_prefix + "::run: " + "merging histograms")

            # > for backwards compatibility
            if single_file:
                # > unroll the single histogram to all registered observables
                singles: list[GenericPath] = in_files.pop(single_file)
                in_files = dict((obs, singles) for obs in self.config["run"]["histograms"].keys())

            # > merge all histograms
            # > keep track of all cross section estimates (also as sums over distributions)
            cross_list: list[tuple[float, float]] = []
            for obs, hist_info in self.config["run"]["histograms"].items():
                out_file: Path = mrg_path / f"{obs}.dat"
                nx: int = hist_info["nx"]
                qwgt: bool = self.grids and (hist_info.get("grid") is not None)
                container = NNLOJETContainer(size=len(in_files[obs]), weights=qwgt)
                obs_name: str | None = obs if single_file else None
                for in_file in in_files[obs]:
                    try:
                        container.append(
                            NNLOJETHistogram(
                                nx=nx,
                                filename=self._path / in_file,
                                obs_name=obs_name,
                                weights=qwgt,
                            )
                        )
                    except ValueError as e:
                        self._logger(session, f"error reading file {in_file} ({e!r})", level=LogLevel.ERROR)
                container.mask_outliers(
                    self.config["merge"]["trim_threshold"],
                    self.config["merge"]["trim_max_fraction"],
                )
                container.optimise_k(
                    maxdev_unwgt=None,
                    nsteps=self.config["merge"]["k_scan_nsteps"],
                    maxdev_steps=self.config["merge"]["k_scan_maxdev_steps"],
                )
                hist = container.merge(weighted=True)
                hist.write_to_file(out_file)
                if qwgt:
                    weights_file = out_file.with_suffix(".weights.txt")
                    weights_file.write_text(hist.to_weights())

                # > register cross section numbers
                if "cumulant" in hist_info:
                    continue  # @todo ?

                res, err = 0.0, 0.0  # accumulate bins to "cross" (possible fac, selectors, ...)
                if nx == 0:
                    with open(out_file) as cross:
                        for line in cross:
                            if line.startswith("#"):
                                continue
                            col: list[float] = [float(c) for c in line.split()]
                            res = col[0]
                            err = col[1] ** 2
                            break
                elif nx == 3:
                    with open(out_file) as diff:
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
                    pt.result = res
                    pt.error = err

                self._debug(
                    session,
                    self._logger_prefix + f"::run:  {obs:>15}[{nx}]:  {res} +/- {err}",
                )
                cross_list.append((res, err))

                # # > override error if larger from bin sums (correaltions with counter-events)
                # if err > pt.error:
                #     pt.error = err

            opt_target: str = self.config["run"]["opt_target"]

            # > different estimates for the relative cross uncertainties
            rel_cross_err: float = 0.0  # default
            if pt.result != 0.0:
                rel_cross_err = abs(pt.error / pt.result)
            elif pt.error != 0.0:
                raise ValueError(self._logger_prefix + f"::run:  val={pt.result}, err={pt.error}")
            cross_list.append((1.0, 0.0))  # safe guard against all-zero case
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
            # > override with registered error with the optimization target
            pt.error = abs(rel_cross_err * pt.result)

            self._safe_commit(session)

            # @todo keep track of a "settings.json" for merge settings used?
            # with open(out_file, "w") as out:
            #     for in_file in in_files:
            #         out.write(str(in_file))

        if not self.force:
            yield self.clone(cls=MergeAll)


class MergeAll(DBMerge):
    # > merge all `Part` objects that are currently active

    priority = 110

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger_prefix: str = "MergeAll"
        with self.session as session:
            if self.force or self.reset_tag > 0.0:
                self._logger_prefix = self._logger_prefix + f"[force={self.force}, reset={time.ctime(self.reset_tag)}]"
            self._debug(session, self._logger_prefix + "::init")
        # > output directory
        self.mrg_path: Path = self._path.joinpath("result", "merge")
        if not self.mrg_path.exists():
            self.mrg_path.mkdir(parents=True)

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

    def complete(self) -> bool:
        # > check input requirements
        if any(not mpt.complete() for mpt in self.requires()):
            return False

        # > check file modifiation time
        timestamp: float = -1.0
        for hist in os.scandir(self.mrg_path):
            timestamp = max(timestamp, hist.stat().st_mtime)
        if self.run_tag > timestamp:
            return False

        with self.session as session:
            self._debug(
                session,
                self._logger_prefix + f"::complete:  files {datetime.datetime.fromtimestamp(timestamp)}",
            )
            for pt in session.scalars(self.select_part):
                self._debug(
                    session,
                    self._logger_prefix + f"::complete:  {pt.name} {datetime.datetime.fromtimestamp(pt.timestamp)}",
                )
                if pt.timestamp > timestamp:
                    return False
            return True

    def run(self):
        with self.session as session:
            self._logger(session, self._logger_prefix + "::run")
            mrg_parent: Path = self._path.joinpath("result", "part")

            # > collect all input files
            in_files = dict((obs, []) for obs in self.config["run"]["histograms"].keys())
            # > reconstruct optimisation target
            opt_target: str = self.config["run"]["opt_target"]
            opt_target_ref: float = 0.0
            opt_target_rel: float = 0.0
            for pt in session.scalars(self.select_part):
                opt_target_ref += pt.result
                opt_target_rel += pt.error**2
                for obs in self.config["run"]["histograms"]:
                    in_file: Path = mrg_parent / pt.name / f"{obs}.dat"
                    if in_file.exists():
                        in_files[obs].append(str(in_file.relative_to(self._path)))
                    else:
                        raise FileNotFoundError(f"MergeAll::run:  missing {in_file}")
            opt_target_rel = math.sqrt(opt_target_rel) / abs(opt_target_ref)  # relative uncertainty

            # > use `distribute_time` to fetch optimization target
            # > use small 1s value; a non-zero time to avoid division by zero
            # > the above does not include penalty, which is why we override it this way
            opt_dist = self._distribute_time(session, 1.0)
            opt_target_ref = opt_dist["tot_result"]
            opt_target_rel = abs(opt_dist["tot_error"] / opt_dist["tot_result"])

            # > sum all parts
            for obs, hist_info in self.config["run"]["histograms"].items():
                out_file: Path = self.mrg_path / f"{obs}.dat"
                nx: int = hist_info["nx"]
                qwgt: bool = self.grids and (hist_info.get("grid") is not None)
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

            # > sum all parts for `*.dat2`
            for obs, hist_info in self.config["run"]["histograms"].items():
                out_file: Path = self.mrg_path / f"{obs}.dat2"
                self._logger(session, f"{self._logger_prefix}::run:  merging {obs} to {out_file}", level=LogLevel.WARN)
                nx: int = hist_info["nx"]
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
                        hist = hist + NNLOJETHistogram(nx=nx, filename=self._path / (str(in_file) + "2"))
                    except ValueError as e:
                        self._logger(session, f"error reading file {in_file} ({e!r})", level=LogLevel.ERROR)
                hist.write_to_file(out_file)


class MergeFinal(DBMerge):
    # > a final merge of all orders where we have parts available

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger_prefix: str = "MergeFinal"
        with self.session as session:
            if self.force or self.reset_tag > 0.0:
                self._logger_prefix = self._logger_prefix + f"[force={self.force}, reset={time.ctime(self.reset_tag)}]"
            self._debug(session, self._logger_prefix + "::init")

        # > output directory
        self.fin_path: Path = self._path.joinpath("result", "final")
        if not self.fin_path.exists():
            self.fin_path.mkdir(parents=True)

        self.result = float("nan")
        self.error = float("inf")

    def requires(self):
        with self.session as session:
            self._debug(session, self._logger_prefix + "::requires")
        return [self.clone(MergeAll, force=True)]

    def complete(self) -> bool:
        with self.session as session:
            self._debug(session, self._logger_prefix + "::complete")
            last_sig = session.scalars(select(Log).where(Log.level < 0).order_by(Log.id.desc())).first()
            self._debug(session, self._logger_prefix + f"::complete:  last_sig = {last_sig!r}")
            if last_sig and last_sig.level in [LogLevel.SIG_COMP]:
                return True
        return False

    def run(self):
        with self.session as session:
            self._logger(session, self._logger_prefix + "::run")
            mrg_parent: Path = self._path.joinpath("result", "part")

            # > create "final" files that merge parts into the different orders that are complete
            for out_order in Order:
                select_order = select(Part)  # no need to be active: .where(Part.active.is_(True))
                if int(out_order) < 0:
                    select_order = select_order.where(Part.order == out_order)
                else:
                    select_order = select_order.where(func.abs(Part.order) <= out_order)
                matched_parts = session.scalars(select_order).all()

                # > is there even a Part at this order for this process? (NNLO for an NLO-only process)
                if session.query(Part).filter(func.abs(Part.order) == abs(out_order)).count() == 0:
                    self._logger(session, self._logger_prefix + f"::run:  no parts at order {out_order}")
                    continue

                # > in order to write out an `order` result, we need at least one complete result for each part
                if any(pt.ntot <= 0 for pt in matched_parts):
                    self._logger(
                        session,
                        f'[red]MergeFinal::run:  skipping "{out_order}" due to incomplete parts[/red]',
                    )
                    continue

                self._debug(
                    session,
                    self._logger_prefix + f"::run:  {out_order}: {list(map(lambda x: (x.id, x.ntot), matched_parts))}",
                )

                in_files = dict((obs, []) for obs in self.config["run"]["histograms"].keys())
                for pt in matched_parts:
                    for obs in self.config["run"]["histograms"]:
                        in_file: Path = mrg_parent / pt.name / f"{obs}.dat"
                        if in_file.exists():
                            in_files[obs].append(str(in_file.relative_to(self._path)))
                        else:
                            raise FileNotFoundError(f"MergeFinal::run:  missing {in_file}")

                # > sum all parts
                for obs, hist_info in self.config["run"]["histograms"].items():
                    out_file: Path = self.fin_path / f"{out_order}.{obs}.dat"
                    nx: int = hist_info["nx"]
                    qwgt: bool = self.grids and (hist_info.get("grid") is not None)
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
                            self._logger(
                                session,
                                self._logger_prefix + f"::run:  error reading file {in_file} ({e!r})",
                                level=LogLevel.ERROR,
                            )
                    hist.write_to_file(out_file)
                    if qwgt:
                        weights_file = out_file.with_suffix(".weights.txt")
                        weights_file.write_text(hist.to_weights())
                        pine_merge: Path = Path(self.config["exe"]["path"]).parent / "nnlojet-merge-pineappl"
                        if pine_merge.is_file() and os.access(pine_merge, os.X_OK):
                            grid_file: Path = out_file.with_suffix(".pineappl.lz4")
                            grid_log: Path = out_file.with_suffix(".pineappl.log")
                            job_env = os.environ.copy()
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
                            pass
                        else:
                            self._logger(
                                session,
                                f"[red]MergeFinal::run:  missing nnlojet-merge-pineappl executable at {pine_merge}[/red]",
                                level=LogLevel.ERROR,
                            )

                # > sum all `*.dat2` files
                for obs, hist_info in self.config["run"]["histograms"].items():
                    out_file: Path = self.fin_path / f"{out_order}.{obs}.dat2"
                    self._logger(session, f"{self._logger_prefix}::run:  merging {obs} to {out_file}", level=LogLevel.WARN)
                    nx: int = hist_info["nx"]
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
                            hist = hist + NNLOJETHistogram(nx=nx, filename=self._path / (str(in_file) + "2"))
                        except ValueError as e:
                            self._logger(
                                session,
                                self._logger_prefix + f"::run:  error reading file {in_file} ({e!r})",
                                level=LogLevel.ERROR,
                            )
                    hist.write_to_file(out_file)

            # > shut down the monitor
            self._logger(session, "complete", level=LogLevel.SIG_COMP)
            time.sleep(2.0 * self.config["ui"]["refresh_delay"])

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
                f"\n[blue]cross = ({self.result} +/- {self.error}) fb  [{rel_acc * 1e2:.3}%][/blue]" + f"\n[dim](total runtime invested: {format_time_interval(T_tot)})[/dim]",
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
                    f"[green]reached rel. acc. {rel_acc * 1e2:.3}% on {opt_target}[/green] " + f"(requested: {self.config['run']['target_rel_acc'] * 1e2:.3}%)",
                )
            else:
                self._logger(
                    session,
                    f"[red]reached rel. acc. {rel_acc * 1e2:.3}% on {opt_target}[/red] " + f"(requested: {self.config['run']['target_rel_acc'] * 1e2:.3}%)",
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
                    "still require about " + f"[bold]{format_time_interval(T_target)}[/bold]" + " of runtime to reach desired target accuracy" + f" [dim](approx. {njobs_target} jobs)[/dim]",
                )
