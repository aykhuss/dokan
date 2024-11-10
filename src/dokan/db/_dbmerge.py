import luigi
import time
import datetime
import re
import os

from abc import ABCMeta, abstractmethod
from pathlib import Path
from sqlalchemy import select

from ..exe._exe_config import ExecutionMode
from ..exe._exe_data import ExeData
from ._sqla import Part, Job
from ._jobstatus import JobStatus
from ._loglevel import LogLevel
from ._dbtask import DBTask
from ..combine import NNLOJETHistogram, NNLOJETContainer


class DBMerge(DBTask, metaclass=ABCMeta):
    # > flag to force a re-merge
    force: bool = luigi.BoolParameter(default=False)

    # > limit the resources on local cores
    # @todo make common
    resources = {"DBMerge": 1}

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

            self.debug(f"MergePart::complete[{self.part_id},{self.force}]: done {c_done}, merged {c_merged}")

            if self.force and c_done > 0:
                return False

            if float(c_done) / float(c_merged + 1) <= 1.0:  # @todo make config parameter?
                return True
        return False

    def run(self):
        self.logger(f"MergePart::run[{self.part_id}]")
        with self.session as session:
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
                self.debug(f"MergePart::run[{self.part_id}] appending {job!r}")
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
            for obs in self.config["run"]["histograms"]:
                out_file: Path = mrg_path / f"{obs}.dat"
                nx: int = self.config["run"]["histograms"][obs]["nx"]
                container = NNLOJETContainer(size=len(in_files[obs]))
                for in_file in in_files[obs]:
                    try:
                        container.append(NNLOJETHistogram(nx=nx, filename=self._path / in_file))
                    except ValueError as e:
                        self.logger(f"error reading file {in_file} ({e!r})", level=LogLevel.ERROR)
                container.mask_outliers(3.5, 0.01)
                container.optimise_k(maxdev_unwgt=None, nsteps=3, maxdev_steps=0.5)
                hist = container.merge(weighted=True)
                hist.write_to_file(out_file)

                if obs == "cross":
                    with open(out_file, "rt") as cross:
                        for line in cross:
                            if line.startswith("#"):
                                continue
                            pt.result = float(line.split()[0])
                            pt.error = float(line.split()[1])
                            break

            session.commit()

            # @todo keep track of a "settings.json" for merge settings used?
            # with open(out_file, "w") as out:
            #     for in_file in in_files:
            #         out.write(str(in_file))


class MergeAll(DBMerge):
    # > merge all `Part` objects

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # > output directory
        self.fin_path: Path = self._path.joinpath("result", "final")
        if not self.fin_path.exists():
            self.fin_path.mkdir(parents=True)

    @property
    def select_part(self):
        return select(Part).where(Part.active.is_(True))

    def requires(self):
        if self.force:
            self.debug("MergeAll: requires parts...")
            with self.session as session:
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
        for hist in os.scandir(self.fin_path):
            timestamp = max(timestamp, hist.stat().st_mtime)
        if self.run_tag > timestamp:
            return False
        self.debug(f"MergeAll: files {datetime.datetime.fromtimestamp(timestamp)}")
        with self.session as session:
            for pt in session.scalars(self.select_part):
                self.debug(f"MergeAll: {pt.name} {datetime.datetime.fromtimestamp(pt.timestamp)}")
                if pt.timestamp > timestamp:
                    return False
            return True

    def run(self):
        self.logger("MergeAll::run")
        mrg_parent: Path = self._path.joinpath("result", "part")

        with self.session as session:
            in_files = dict((obs, []) for obs in self.config["run"]["histograms"].keys())
            for pt in session.scalars(self.select_part):
                for obs in self.config["run"]["histograms"]:
                    in_file: Path = mrg_parent / pt.name / f"{obs}.dat"
                    if in_file.exists():
                        in_files[obs].append(str(in_file.relative_to(self._path)))
                    else:
                        raise FileNotFoundError(f"MergeAll: missing {in_file}")

            # > sum all parts
            for obs in self.config["run"]["histograms"]:
                out_file: Path = self.fin_path / f"{obs}.dat"
                nx: int = self.config["run"]["histograms"][obs]["nx"]
                hist = NNLOJETHistogram()
                for in_file in in_files[obs]:
                    try:
                        hist = hist + NNLOJETHistogram(nx=nx, filename=self._path / in_file)
                    except ValueError as e:
                        self.logger(f"error reading file {in_file} ({e!r})", level=LogLevel.ERROR)
                hist.write_to_file(out_file)

        self.debug("MergeAll: complete all")
        # self.print_job()
