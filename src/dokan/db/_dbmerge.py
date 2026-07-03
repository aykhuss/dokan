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
import time
from abc import ABCMeta
from pathlib import Path
from typing import cast

import h5py
import luigi
import numpy as np
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .._types import GenericPath
from ..exe._exe_config import ExecutionMode
from ..exe._exe_data import ExeData
from ..merge._core import (
    MergeObs,
    _accumulate_dat,
    _obs_has_grid,
    _run_pineappl_merge,
    _write_dat,
    _write_weights,
    build_obs_group,
)
from ..order import Order
from ..util import format_time_interval, read_json_sidecar, write_json_sidecar
from ._dbtask import DBTask
from ._jobstatus import JobStatus
from ._loglevel import LogLevel
from ._sqla import Job, Log, Part


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

            # > nothing new to merge: the part is fully merged.  Guarding on c_done here
            # > keeps `complete()` independent of `fac_merge_trigger`: the ratio test below
            # > evaluates to exactly 1.0 when c_done == 0, so any `fac_merge_trigger <= 1.0`
            # > would otherwise make a fully-merged part read as incomplete forever (livelock).
            if c_done == 0:
                return True

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
                    # > a successful job without a run directory cannot contribute data and can
                    # > never be marked MERGED below; demote it to FAILED so it leaves the
                    # > success_list the counts filter on (otherwise c_done stays > 0 and a
                    # > forced MergePart/MergeAll never completes -> infinite re-merge loop).
                    self._logger(
                        session,
                        self._logger_prefix
                        + f"::run:  job {job.id} is {JobStatus(job.status)!s} without rel_path => FAILED",
                        level=LogLevel.WARN,
                    )
                    job.status = JobStatus.FAILED
                    continue
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
        # @todo: better to save sumf & sumf2? more convenient for unweighted combination and the k-scan.
        # Could start by storing res & err, then switch to sumf & sumf2 later.
        # Maybe an attribute to flag what of the two is stored? Helper routine to convert could also help.
        resize_max: int = max(len(files) for files in in_files.values()) if in_files else 0
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
            build_obs_group(
                hdf5_file,
                pt_name,
                in_files,
                self.config["run"]["histograms"],
                self._path,
                single_file=single_file,
                merge_in_progress=merge_in_progress,
            )

        # > find all obs that have data in the HDF5 file
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

        # > dispatch one MergeObs per observable that has data: MergeObs.complete() is the single
        # > freshness authority (new data, missing/stale `.dat`, reset epoch, merge-config change,
        # > or missing grid artifacts), so we hand it *every* ready observable rather than
        # > second-guessing which ones are stale here.
        # > cast: clone() is typed to return the parent task type, so annotate the MergeObs values
        # > explicitly to keep `expected_identity()`/`required_outputs()` resolvable below.
        mrg_obs_dict = {
            obs: cast(
                MergeObs,
                self.clone(
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
                ),
            )
            for obs, hist_info in self.config["run"]["histograms"].items()
            if obs in hdf5_obs_ready
        }
        pending_obs = {obs: mrg_obs for obs, mrg_obs in mrg_obs_dict.items() if not mrg_obs.complete()}
        # > Stall-guard state must live on disk: with `workers > 1` Luigi forks a fresh
        # > process for every run() attempt, so instance attributes do not survive the
        # > restart after the yielded MergeObs complete.
        guard_file: Path = self._path / "raw" / f"{pt_name}.merge-guard.json"
        if pending_obs:
            # > Stall guard: Luigi restarts run() from the top each time the yielded MergeObs
            # > complete, so an observable that never reports complete() would have us re-yield it
            # > forever, never reaching the phase below that clears the in-progress timestamp.
            # > Fingerprint the pending set by each observable's `expected_identity()`; any genuine
            # > change (new data, reset epoch, config, grids) alters it, so a verbatim repeat means
            # > the previous merge ran and still did not converge.
            # > (JSON round-trip normalizes tuples to lists so both sides compare equal.)
            pending_identity = json.loads(
                json.dumps(
                    sorted((obs, mrg_obs.expected_identity()) for obs, mrg_obs in pending_obs.items())
                )
            )
            prior_guard = read_json_sidecar(guard_file)
            if prior_guard is not None and prior_guard.get("pending_identity") == pending_identity:
                # > Invariant: re-merging an unchanged pending set must converge.  Identity is
                # > deterministic (sidecar vs. live HDF5/config, no mtime), so reaching here means
                # > `MergeObs.run()` produced outputs its own `complete()` still rejects — a bug in
                # > the merge or its freshness model, or a corrupt/failed sidecar write.  Fail loudly
                # > with per-observable diagnostics rather than loop forever or finalize a bad part.
                # > (Rare false positive: a crash after the guard write but before MergeObs ran;
                # > deleting the guard file forces a retry.)
                diagnostics = "; ".join(
                    f"{obs}: {mrg_obs.describe_incomplete()}" for obs, mrg_obs in sorted(pending_obs.items())
                )
                raise RuntimeError(
                    self._logger_prefix
                    + "::run:  internal invariant violated: re-merge of unchanged pending set "
                    + f"{sorted(pending_obs)} for part {pt_name} did not converge (bug in MergeObs "
                    + f"freshness or merge); refusing to finalize.  Diagnostics: {diagnostics}  "
                    + f"(delete {guard_file} to force a retry after investigating)"
                )
            write_json_sidecar(guard_file, {"pending_identity": pending_identity})
            yield list(pending_obs.values())
        # > merge converged for the current inputs: retire the guard fingerprint
        guard_file.unlink(missing_ok=True)

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
                + f"::run: {max_rel_hist_err=}  pt.result = {pt.result} +/- {pt.error}"
                + f" (rel_err = {rel_cross_err:.3e})",
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
                acc = _accumulate_dat(
                    in_files[obs],
                    nx,
                    self._path,
                    on_error=lambda f, e: self._logger(
                        session, f"error reading file {f} ({e!r})", level=LogLevel.ERROR
                    ),
                )
                if acc is None:
                    self._logger(
                        session,
                        self._logger_prefix + f"::run:  no usable files for {obs}",
                        level=LogLevel.ERROR,
                    )
                    continue
                labels, neval, xval, hist, used = acc
                _write_dat(out_file, labels, neval, nx, xval, hist)
                written_observables.append(obs)
                if qwgt:
                    weights_file = out_file.with_suffix(".weights.txt")
                    filenames = [(self._path / f).as_posix() for f in used]
                    weights = np.ones((hist.shape[0], len(filenames)), dtype=np.float64)
                    _write_weights(weights_file, nx, xval, filenames, weights)
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
                                + f"{opt_target_rel * 1e2:.3}%"
                                + f" (requested: {self.config['run']['target_rel_acc'] * 1e2:.3}%)[/magenta]",
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

                    # > writing an `order` result requires at least one complete result for each part
                    if any(pt.ntot <= 0 for pt in matched_parts):
                        self._logger(
                            session,
                            f'[red]{self._logger_prefix}::run:  skipping "{out_order}"'
                            + " due to incomplete parts[/red]",
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
                        acc = _accumulate_dat(
                            in_files_fin[obs],
                            nx,
                            self._path,
                            on_error=lambda f, e: self._logger(
                                session,
                                self._logger_prefix + f"::run:  error reading file {f} ({e!r})",
                                level=LogLevel.ERROR,
                            ),
                        )
                        if acc is None:
                            self._logger(
                                session,
                                self._logger_prefix + f"::run:  no usable files for {obs}",
                                level=LogLevel.ERROR,
                            )
                            continue
                        labels, neval, xval, hist, used = acc
                        _write_dat(out_file, labels, neval, nx, xval, hist)
                        if qwgt:
                            weights_file = out_file.with_suffix(".weights.txt")
                            filenames = [(self._path / f).as_posix() for f in used]
                            weights = np.ones((hist.shape[0], len(filenames)), dtype=np.float64)
                            _write_weights(weights_file, nx, xval, filenames, weights)
                            pine_merge: Path = (
                                Path(self.config["exe"]["path"]).parent / "nnlojet-merge-pineappl"
                            )
                            if pine_merge.is_file() and os.access(pine_merge, os.X_OK):
                                # > all parts ready -> combine into final grid
                                grid_file: Path = out_file.with_suffix(".pineappl.lz4")
                                _run_pineappl_merge(pine_merge, weights_file, grid_file, check=False)
                            else:
                                self._logger(
                                    session,
                                    f"[red]{self._logger_prefix}::run:"
                                    + "  missing nnlojet-merge-pineappl executable"
                                    + f"  at {pine_merge}[/red]",
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
        def safe_rel(error: float, result: float) -> float:
            """Relative accuracy that tolerates a vanishing result (maps to inf)."""
            return abs(error / result) if result != 0.0 else float("inf")

        with self.session as session:
            self._logger(session, self._logger_prefix + "::run")

            # > parse merged cross section result
            mrg_all: MergeAll = self.requires()[0]
            dat_cross: Path = mrg_all.mrg_path / "cross.dat"
            if not dat_cross.is_file():
                # > fail *before* SIG_COMP is written so the workflow is not marked complete
                self._logger(
                    session,
                    self._logger_prefix + f"::run:  missing merged cross section {dat_cross}",
                    level=LogLevel.ERROR,
                )
                raise FileNotFoundError(f"{self._logger_prefix}::run: missing {dat_cross}")
            with open(dat_cross) as cross:
                for line in cross:
                    if line.startswith("#"):
                        continue
                    self.result = float(line.split()[0])
                    self.error = float(line.split()[1])
                    break
            rel_acc: float = safe_rel(self.error, self.result)
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
            rel_acc: float = safe_rel(opt_dist["tot_error"], opt_dist["tot_result"])
            if rel_acc <= self.config["run"]["target_rel_acc"] * (1.05):
                self._logger(
                    session,
                    f"[green]reached rel. acc. {rel_acc * 1e2:.3}% on {opt_target}[/green]"
                    + f" (requested: {self.config['run']['target_rel_acc'] * 1e2:.3}%)",
                )
            else:
                self._logger(
                    session,
                    f"[red]reached rel. acc. {rel_acc * 1e2:.3}% on {opt_target}[/red]"
                    + f" (requested: {self.config['run']['target_rel_acc'] * 1e2:.3}%)",
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
                    "still require about"
                    + f" [bold]{format_time_interval(T_target)}[/bold]"
                    + " of runtime to reach desired target accuracy"
                    + f" [dim](approx. {njobs_target} jobs)[/dim]",
                )

            # > mark the workflow complete (also shuts down the monitor).  Written *last*
            # > so that a failure anywhere above leaves the run incomplete instead of
            # > masking the error as success (`complete()` keys off this signal).
            self._logger(session, "complete", level=LogLevel.SIG_COMP)
            time.sleep(self.config["ui"]["refresh_delay"])
