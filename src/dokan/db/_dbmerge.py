"""dokan merge tasks

defines tasks to merge individual NNLOJET results into a combined result.
constitutes the dokan workflow implementation of `nnlojet-combine.py`
"""

import datetime
import math
import os
import re
import time
from abc import ABCMeta
from pathlib import Path

import luigi
from sqlalchemy import select

from ..combine import NNLOJETContainer, NNLOJETHistogram
from ..exe._exe_config import ExecutionMode
from ..exe._exe_data import ExeData
from ._dbtask import DBTask
from ._jobstatus import JobStatus
from ._loglevel import LogLevel
from ._sqla import Job, Part


class DBMerge(DBTask, metaclass=ABCMeta):
    # > flag to force a re-merge
    force: bool = luigi.BoolParameter(default=False)

    priority = 20

    # > limit the resources on local cores
    resources = {"local_ncores": 1}

    # @staticmethod
    # def clone_factory(orig: DBTask, id: int = 0):
    #     if id > 0:
    #         return orig.clone(cls=MergePart, part_id=id)
    #     else:
    #         return orig.clone(cls=MergeAll)

    # @property
    # @abstractmethod
    # def select_part(self):
    #     return select(Part)

    # def update_timestamp(self) -> None:
    #     with self.session as session:
    #         timestamp: float = time.time()
    #         for pt in session.scalars(self.select_part):
    #             pt.timestamp = timestamp
    #         session.commit()


class MergePart(DBMerge):
    # > merge only a specific `Part`
    part_id: int = luigi.IntParameter()

    # @property
    # def select_part(self):
    #     return select(Part).where(Part.id == self.part_id).where(Part.active.is_(True))

    @property
    def select_job(self):
        return (
            select(Job)
            .join(Part)
            .where(Part.id == self.part_id)
            .where(Part.active.is_(True))
            .where(Job.mode == ExecutionMode.PRODUCTION)
            .where(Job.status.in_(JobStatus.success_list()))
            .where(Job.timestamp < Part.timestamp)
        )

    def complete(self) -> bool:
        with self.session as session:
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
                # @todo raise error as we should never be in this situation?
                return True

            self._debug(
                session,
                f"MergePart::complete[{self.part_id},{self.force}]: done {c_done}, merged {c_merged}",
            )

            if self.force and c_done > 0:
                return False

            if float(c_done + c_merged) / float(c_merged + 1) <= (
                self.config["production"]["fac_merge_trigger"]
                if "fac_merge_trigger" in self.config["production"]
                else 2.0
            ):
                return True

            self._logger(
                session,
                f"MergePart::complete[{self.part_id},{self.force}]:  "
                + f"done {c_done}, merged {c_merged} => not yet complete",
            )

        return False

    def run(self):
        if self.complete():
            return

        with self.session as session:
            self._debug(session, f"MergePart::run[{self.part_id},{self.force}]")
            # > get the part and update timestamp to tag for 'MERGE'
            pt: Part = session.get_one(Part, self.part_id)
            pt.timestamp = time.time()
            session.commit()

            # > output directory
            mrg_path: Path = self._path.joinpath("result", "part", pt.name)
            if not mrg_path.exists():
                mrg_path.mkdir(parents=True)

            # > populate a dictionary with all histogram files (reduces IO)
            if "histograms_single_file" in self.config["run"]:  # @todo
                raise NotImplementedError("MergePart: histograms_single_file")
            in_files = dict((obs, []) for obs in self.config["run"]["histograms"].keys())
            # > collect histograms from all jobs
            pt.Ttot = 0.0
            pt.ntot = 0
            for job in session.scalars(self.select_job):
                if not job.rel_path:
                    continue  # @todo raise warning in logger?
                self._debug(session, f"MergePart::run[{self.part_id}] appending {job!r}")
                pt.Ttot += job.elapsed_time
                pt.ntot += job.niter * job.ncall
                job_path: Path = self._path / job.rel_path
                exe_data = ExeData(job_path)
                for out in exe_data["output_files"]:
                    if dat := re.match(r"^.*\.([^.]+)\.s[0-9]+\.dat", out):
                        if dat.group(1) in in_files:
                            in_files[dat.group(1)].append(
                                str((job_path / out).relative_to(self._path))
                            )
                job.status = JobStatus.MERGED

            # > merge all histograms
            # > keep track of all cross section estimates (also from distributions)
            cross_list: list[tuple[float, float]] = []
            for obs in self.config["run"]["histograms"]:
                out_file: Path = mrg_path / f"{obs}.dat"
                nx: int = self.config["run"]["histograms"][obs]["nx"]
                container = NNLOJETContainer(size=len(in_files[obs]))
                for in_file in in_files[obs]:
                    try:
                        container.append(NNLOJETHistogram(nx=nx, filename=self._path / in_file))
                    except ValueError as e:
                        self._logger(
                            session, f"error reading file {in_file} ({e!r})", level=LogLevel.ERROR
                        )
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

                # > register cross section numbers
                if "cumulant" in self.config["run"]["histograms"][obs]:
                    continue  # @todo ?

                res, err = 0.0, 0.0
                nx: int = self.config["run"]["histograms"][obs]["nx"]
                if nx == 0:
                    with open(out_file, "rt") as cross:
                        for line in cross:
                            if line.startswith("#"):
                                continue
                            col: list[float] = [float(c) for c in line.split()]
                            res = col[0]
                            err = col[1] ** 2
                            break
                elif nx == 3:
                    with open(out_file, "rt") as diff:
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
                    raise ValueError(f"MergePart::run[{self.part_id}]:  unexpected nx = {nx}")
                err = math.sqrt(err)

                if obs == "cross":
                    pt.result = res
                    pt.error = err

                self._debug(
                    session, f"MergePart::run[{self.part_id}]:  {obs:>15}[{nx}]:  {res} +/- {err}"
                )
                cross_list.append((res, err))

                # # > override error if larger from bin sums (correaltions with counter-events)
                # if err > pt.error:
                #     pt.error = err

            opt_target: str = (
                self.config["run"]["opt_target"]
                if "opt_target" in self.config["run"]
                else "cross_hist"  # default
            )

            # > this is an upper bound on the XS error derived from any histogram
            # > ("+ pt.error" accounts for the worst case with histogram selectors)
            max_err: float = pt.error + max(e for _, e in cross_list)
            if opt_target == "cross":
                pass  # keep cross error for optimisation
            elif opt_target == "cross_hist":
                # > since we took the worst case for max_err, let's take a geometric mean
                # pt.error = (pt.error + max_err) / 2.0
                pt.error = math.sqrt(pt.error * max_err)
            elif opt_target == "hist":
                pt.error = max_err
            else:
                raise ValueError(
                    f"MergePart::run[{self.part_id}]:  unknown opt_target {opt_target}"
                )

            session.commit()

            # @todo keep track of a "settings.json" for merge settings used?
            # with open(out_file, "w") as out:
            #     for in_file in in_files:
            #         out.write(str(in_file))

        if not self.force:
            yield self.clone(cls=MergeAll)


class MergeAll(DBMerge):
    # > merge all `Part` objects

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # > output directory
        self.mrg_path: Path = self._path.joinpath("result", "merge")
        if not self.mrg_path.exists():
            self.mrg_path.mkdir(parents=True)

    @property
    def select_part(self):
        return select(Part).where(Part.active.is_(True))

    def requires(self):
        if self.force:
            with self.session as session:
                self._debug(session, "MergeAll: requires parts...")
                return [
                    self.clone(cls=MergePart, part_id=pt.id)
                    for pt in session.scalars(self.select_part)
                ]
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
            self._debug(session, f"MergeAll: files {datetime.datetime.fromtimestamp(timestamp)}")
            for pt in session.scalars(self.select_part):
                self._debug(
                    session, f"MergeAll: {pt.name} {datetime.datetime.fromtimestamp(pt.timestamp)}"
                )
                if pt.timestamp > timestamp:
                    return False
            return True

    def run(self):
        with self.session as session:
            self._logger(session, f"MergeAll::run[force={self.force}]")
            mrg_parent: Path = self._path.joinpath("result", "part")

            in_files = dict((obs, []) for obs in self.config["run"]["histograms"].keys())
            for pt in session.scalars(self.select_part):
                for obs in self.config["run"]["histograms"]:
                    in_file: Path = mrg_parent / pt.name / f"{obs}.dat"
                    if in_file.exists():
                        in_files[obs].append(str(in_file.relative_to(self._path)))
                    else:
                        raise FileNotFoundError(f"MergeAll::run:  missing {in_file}")

            # > sum all parts
            for obs in self.config["run"]["histograms"]:
                out_file: Path = self.mrg_path / f"{obs}.dat"
                nx: int = self.config["run"]["histograms"][obs]["nx"]
                if len(in_files[obs]) == 0:
                    self._logger(
                        session, f"MergeAll::run:  no files for {obs}", level=LogLevel.ERROR
                    )
                    continue
                hist = NNLOJETHistogram()
                for in_file in in_files[obs]:
                    try:
                        hist = hist + NNLOJETHistogram(nx=nx, filename=self._path / in_file)
                    except ValueError as e:
                        self._logger(
                            session, f"error reading file {in_file} ({e!r})", level=LogLevel.ERROR
                        )
                hist.write_to_file(out_file)
                if obs == "cross":
                    with open(out_file, "rt") as cross:
                        for line in cross:
                            if line.startswith("#"):
                                continue
                            col: list[float] = [float(c) for c in line.split()]
                            res: float = col[0]
                            err: float = col[1]
                            rel: float = abs(err / res) if res != 0.0 else float("inf")
                            self._logger(
                                session,
                                f"[blue]cross = ({res} +/- {err}) fb  [{rel * 1e2:.3}%][/blue]",
                                level=LogLevel.SIG_UPDXS,
                            )
                            break
