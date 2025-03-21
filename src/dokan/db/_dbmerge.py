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
from sqlalchemy import func, select

from .._types import GenericPath
from ..combine import NNLOJETContainer, NNLOJETHistogram
from ..exe._exe_config import ExecutionMode
from ..exe._exe_data import ExeData
from ..order import Order
from ._dbtask import DBTask
from ._jobstatus import JobStatus
from ._loglevel import LogLevel
from ._sqla import Job, Log, Part


class DBMerge(DBTask, metaclass=ABCMeta):
    # > flag to force a re-merge (if new jobs are in a `done` state but not yet `merged`)
    force: bool = luigi.BoolParameter(default=False)
    # > tag to trigger a reset to initiate a re-merge from scratch (timestamp)
    reset_tag: float = luigi.FloatParameter(default=-1.0)

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
                self._debug(
                    session,
                    f"MergePart::complete[{self.part_id},{self.force},{self.reset_tag}]: done {c_done}, merged {c_merged} => mark complete",
                )
                # @todo raise error as we should never be in this situation?
                return True

            self._debug(
                session,
                f"MergePart::complete[{self.part_id},{self.force},{self.reset_tag}]: done {c_done}, merged {c_merged}",
            )

            if self.force and c_done > 0:
                return False

            pt: Part = session.get_one(Part, self.part_id)
            if pt.timestamp < self.reset_tag:
                return False

            # > this is incorrect, as we need to wait for *all* pre-productions to be complete
            # > before we can merge. The merge is triggered manually in the `Entry` task
            # if c_merged == 0 and c_done > 0:
            #     return False

            if float(c_done + c_merged) / float(c_merged + 1) < (
                self.config["production"]["fac_merge_trigger"]
                if "fac_merge_trigger" in self.config["production"]
                else 2.0
            ):
                return True

            self._logger(
                session,
                f"MergePart::complete[{self.part_id},{self.force},{self.reset_tag}]:  "
                + f"done {c_done}, merged {c_merged} => not yet complete",
            )

        return False

    def run(self):
        if self.complete():
            with self.session as session:
                self._debug(
                    session,
                    f"MergePart::run[{self.part_id},{self.force},{self.reset_tag}]:  already complete",
                )
            return

        with self.session as session:
            self._debug(session, f"MergePart::run[{self.part_id},{self.force},{self.reset_tag}]")
            # > get the part and update timestamp to tag for 'MERGE'
            pt: Part = session.get_one(Part, self.part_id)
            pt.timestamp = time.time()
            session.commit()

            # > output directory
            mrg_path: Path = self._path.joinpath("result", "part", pt.name)
            if not mrg_path.exists():
                mrg_path.mkdir(parents=True)

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
            if single_file:
                # > unroll the single histogram to all registered observables
                singles: list[GenericPath] = in_files.pop(single_file)
                in_files = dict((obs, singles) for obs in self.config["run"]["histograms"].keys())

            # > merge all histograms
            # > keep track of all cross section estimates (also from distributions)
            cross_list: list[tuple[float, float]] = []
            for obs in self.config["run"]["histograms"]:
                out_file: Path = mrg_path / f"{obs}.dat"
                nx: int = self.config["run"]["histograms"][obs]["nx"]
                container = NNLOJETContainer(size=len(in_files[obs]))
                obs_name: str | None = obs if single_file else None
                for in_file in in_files[obs]:
                    try:
                        container.append(
                            NNLOJETHistogram(
                                nx=nx, filename=self._path / in_file, obs_name=obs_name
                            )
                        )
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
            # > alternative: rescale relative errors to the same cross section
            # max_err: float = pt.result * max(abs(e / r) for r, e in cross_list)
            if opt_target == "cross":
                pass  # keep cross error for optimisation
            elif opt_target == "cross_hist":
                # pt.error = (pt.error + max_err) / 2.0
                # > since we took the worst case for max_err, let's take a geometric mean
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
    # > merge all `Part` objects that are currently active

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        with self.session as session:
            self._debug(session, f"MergeAll::init[force={self.force},reset_tag={self.reset_tag}]")
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
            self._logger(session, f"MergeAll::run[force={self.force},reset_tag={self.reset_tag}]")
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


class MergeFinal(DBMerge):
    # > a final merge of all orders where we have parts available

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        with self.session as session:
            self._debug(session, f"MergeFinal::init[force={self.force},reset_tag={self.reset_tag}]")

        # > output directory
        self.fin_path: Path = self._path.joinpath("result", "final")
        if not self.fin_path.exists():
            self.fin_path.mkdir(parents=True)

        self.result = float("nan")
        self.error = float("inf")

    def requires(self):
        with self.session as session:
            self._debug(
                session, "MergeFinal::requires[force={self.force},reset_tag={self.reset_tag}]"
            )
        return [self.clone(MergeAll, force=True)]

    def complete(self) -> bool:
        with self.session as session:
            self._debug(
                session, "MergeFinal::complete[force={self.force},reset_tag={self.reset_tag}]"
            )
            last_sig = session.scalars(
                select(Log).where(Log.level < 0).order_by(Log.id.desc())
            ).first()
            self._debug(session, f"MergeFinal::complete:  last_sig = {last_sig!r}")
            if last_sig and last_sig.level in [LogLevel.SIG_COMP]:
                return True
        return False

    def run(self):
        with self.session as session:
            self._logger(session, f"MergeFinal::run[force={self.force},reset_tag={self.reset_tag}]")
            mrg_parent: Path = self._path.joinpath("result", "part")

            # > create "final" files that merge parts into the different orders that are complete
            for out_order in Order:
                select_order = select(Part)  # no need to be active: .where(Part.active.is_(True))
                if int(out_order) < 0:
                    select_order = select_order.where(Part.order == out_order)
                else:
                    select_order = select_order.where(func.abs(Part.order) <= out_order)
                matched_parts = session.scalars(select_order).all()

                # > in order to write out an `order` result, we need at least one complete result for each part
                if any(pt.ntot <= 0 for pt in matched_parts):
                    self._logger(
                        session,
                        f'[red]MergeFinal::run:  skipping "{out_order}" due to missing parts[/red]',
                    )
                    continue

                self._debug(
                    session, f"{out_order}: {list(map(lambda x: (x.id, x.ntot), matched_parts))}"
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
                for obs in self.config["run"]["histograms"]:
                    out_file: Path = self.fin_path / f"{out_order}.{obs}.dat"
                    nx: int = self.config["run"]["histograms"][obs]["nx"]
                    if len(in_files[obs]) == 0:
                        self._logger(
                            session, f"MergeFinal::run:  no files for {obs}", level=LogLevel.ERROR
                        )
                        continue
                    hist = NNLOJETHistogram()
                    for in_file in in_files[obs]:
                        try:
                            hist = hist + NNLOJETHistogram(nx=nx, filename=self._path / in_file)
                        except ValueError as e:
                            self._logger(
                                session,
                                f"error reading file {in_file} ({e!r})",
                                level=LogLevel.ERROR,
                            )
                    hist.write_to_file(out_file)

            # > shut down the monitor
            self._logger(session, "complete", level=LogLevel.SIG_COMP)
            time.sleep(1.5)

            # > parse merged cross section result
            mrg_all: MergeAll = self.requires()[0]
            dat_cross: Path = mrg_all.mrg_path / "cross.dat"
            with open(dat_cross, "rt") as cross:
                for line in cross:
                    if line.startswith("#"):
                        continue
                    self.result = float(line.split()[0])
                    self.error = float(line.split()[1])
                    break
            rel_acc: float = abs(self.error / self.result)
            self._logger(
                session,
                f"\n[blue]cross = ({self.result} +/- {self.error}) fb  [{rel_acc * 1e2:.3}%][/blue]\n",
            )

            # > use `distribute_time` to fetch optimization target
            # > & time estimate to reach desired accuracy
            # > use small 1s value; a non-zero time to avoid division by zero
            opt_dist = self._distribute_time(session, 1.0)
            # self._logger(session,f"{opt_dist}")
            opt_target: str = (
                self.config["run"]["opt_target"]
                if "opt_target" in self.config["run"]
                else "cross_hist"  # default
            )
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
                njobs_target: int = (
                    round(opt_dist["T_target"] / self.config["run"]["job_max_runtime"]) + 1
                )
                # @todo get a more accurate estimate for number of jobs needed by mimicking a submission?
                self._logger(
                    session,
                    f"still need about [bold]{njobs_target}[/bold] jobs [dim](run time: {self.config['run']['job_max_runtime']}s)[/dim] to reach desired target accuracy.",
                )
