"""dokan merge tasks

defines tasks to merge individual NNLOJET results into a combined result.
constitutes the dokan workflow implementation of `nnlojet-combine.py`
"""

import datetime
import math
import os
import re
import subprocess
import time
from abc import ABCMeta
from pathlib import Path

import luigi
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .._types import GenericPath
from ..combine import NNLOJETContainer, NNLOJETHistogram
from ..exe._exe_config import ExecutionMode
from ..exe._exe_data import ExeData
from ..order import Order
from ..util import format_time_interval
from ._dbtask import DBTask
from ._jobstatus import JobStatus
from ._loglevel import LogLevel
from ._sqla import Job, Log, Part


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
        return (
            self.__class__.__name__
            + "["
            + ", ".join(
                ([f"force={self.force}"] if self.force else [])
                + ([f"reset={time.ctime(self.reset_tag)}"] if self.reset_tag > 0.0 else [])
            )
            + "]"
        )


class MergeObs(luigi.Task):
    ### part_id : the only significant parameter (all other)
    in_files: list[GenericPath] = luigi.ListParameter()
    out_file: GenericPath = luigi.Parameter()
    nx: int = luigi.IntParameter()
    obs_name: str | None = luigi.OptionalParameter(default=None)

    priority = 150

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    # > limit the resources on local cores
    @property
    def resources(self):
        return super().resources | {"local_ncores": 1}

    def complete(self):
        # > check if file exists:
        # -> no: false
        # -> yes:
        #   check the timestamp:
        #   ->
        return False

    def run(self):
        # >
        # print(f"MergeObs:\n{self.in_files}\n{self.out_file}\n{self.nx}\n{self.obs_name}")
        pass


class MergePart(DBMerge):
    # > merge only a specific `Part`
    part_id: int = luigi.IntParameter()

    # > limit the resources on local cores
    @property
    def resources(self):
        return super().resources | {"local_ncores": 1}

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
                    self._logger_prefix
                    + f"::complete:  #done={c_done}, #merged={c_merged} => mark complete",
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
            if (
                self.config["production"]["min_number"] > 0
                and c_done > 0
                and c_merged <= self.config["production"]["min_number"]
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
            self._logger(
                session,
                self._logger_prefix + "::run",
            )

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
                self._debug(
                    session,
                    self._logger_prefix + f"::run:  appending {job!r}",
                )
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
                        else:
                            self._logger(
                                session,
                                self._logger_prefix
                                + "::run:  "
                                + f"unmatched observable {dat.group(1)}?! ({in_files.keys()})",
                            )
                job.status = JobStatus.MERGED
            if single_file:
                # > unroll the single histogram to all registered observables
                singles: list[GenericPath] = in_files.pop(single_file)
                in_files = dict((obs, singles) for obs in self.config["run"]["histograms"].keys())

            # > commit merged status and timestamp before yielding
            pt.timestamp = time.time()
            self._safe_commit(session)

            # # > using MergeObs
            # mrg_obs_list = yield [
            #     MergeObs(
            #         in_files=[str(f) for f in in_files[obs]],
            #         out_file=str((mrg_path / f"{obs}.dat").relative_to(self._path)),
            #         nx=self.config["run"]["histograms"][obs]["nx"],
            #         obs_name=obs if single_file else None,
            #     )
            #     for obs in self.config["run"]["histograms"]
            # ]
            # print(f"{mrg_obs_list!r}")
            # for i, mrg_obs in enumerate(mrg_obs_list, start=1):
            #     print(f"{i}: {mrg_obs}")

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
                if qwgt:
                    weights_file = out_file.with_suffix(".weights.txt")
                    weights_file.write_text(hist.to_weights())

                # > register cross section numbers
                if "cumulant" in hist_info:
                    continue  # @todo ?

                res, err = 0.0, 0.0  # accumulate bins to "cross" (possible fac, selectors, ...)
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
                self._logger_prefix = (
                    self._logger_prefix
                    + f"[force={self.force}, reset={time.ctime(self.reset_tag)}]"
                )
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
            self._debug(
                session,
                self._logger_prefix
                + f"::complete:  files {datetime.datetime.fromtimestamp(timestamp)}",
            )
            for pt in session.scalars(self.select_part):
                self._debug(
                    session,
                    self._logger_prefix
                    + f"::complete:  {pt.name} {datetime.datetime.fromtimestamp(pt.timestamp)}",
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
                        hist = hist + NNLOJETHistogram(
                            nx=nx, filename=self._path / in_file, weights=qwgt
                        )
                    except ValueError as e:
                        self._logger(
                            session, f"error reading file {in_file} ({e!r})", level=LogLevel.ERROR
                        )
                hist.write_to_file(out_file)
                if qwgt:
                    weights_file = out_file.with_suffix(".weights.txt")
                    weights_file.write_text(hist.to_weights())
                if obs == "cross":
                    with open(out_file, "rt") as cross:
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


class MergeFinal(DBMerge):
    # > a final merge of all orders where we have parts available

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger_prefix: str = "MergeFinal"
        with self.session as session:
            if self.force or self.reset_tag > 0.0:
                self._logger_prefix = (
                    self._logger_prefix
                    + f"[force={self.force}, reset={time.ctime(self.reset_tag)}]"
                )
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
            last_sig = session.scalars(
                select(Log).where(Log.level < 0).order_by(Log.id.desc())
            ).first()
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
                    self._logger(
                        session, self._logger_prefix + f"::run:  no parts at order {out_order}"
                    )
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
                    self._logger_prefix
                    + f"::run:  {out_order}: {list(map(lambda x: (x.id, x.ntot), matched_parts))}",
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
                            hist = hist + NNLOJETHistogram(
                                nx=nx, filename=self._path / in_file, weights=qwgt
                            )
                        except ValueError as e:
                            self._logger(
                                session,
                                self._logger_prefix
                                + f"::run:  error reading file {in_file} ({e!r})",
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

            # > shut down the monitor
            self._logger(session, "complete", level=LogLevel.SIG_COMP)
            time.sleep(2.0 * self.config["ui"]["refresh_delay"])

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
            # > compute total runtime invested
            T_tot: float = sum(
                pt.Ttot for pt in session.scalars(select(Part).where(Part.active.is_(True)))
            )
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
