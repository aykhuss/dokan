import luigi
import time
import re

from abc import ABCMeta, abstractmethod
from pathlib import Path
from sqlalchemy import select

from ..exe._exe_config import ExecutionMode
from ..exe._exe_data import ExeData
from ._sqla import Part, Job, JobStatus
from ._dbtask import DBTask
from ..combine import NNLOJETHistogram, NNLOJETContainer


class DBMerge(DBTask, metaclass=ABCMeta):
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

    # @ todo add flag to force re-merge (as long as at least one done job)

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
        # with self.session as session:
        #     for job in session.scalars(self.select_job):
        #         if job.status != JobStatus.MERGED:
        #             return False
        #     return True

        with self.session as session:
            dc_done: int = 0
            dc_merged: int = 0
            for job in session.scalars(self.select_job):
                if job.status == JobStatus.DONE:
                    dc_done += 1
                if job.status == JobStatus.MERGED:
                    dc_merged += 1

            c_done = len(session.scalars(self.select_job.where(Job.status == JobStatus.DONE)).all())
            c_merged = len(
                session.scalars(self.select_job.where(Job.status == JobStatus.MERGED)).all()
            )

            print(
                f"MergePart::complete[{self.part_id}]: done {dc_done}/{c_done}, merged {dc_merged}/{c_merged}"
            )

            if float(c_done) / float(c_merged + 1) < 1.0:  # @todo make config parameter?
                return True
        return False

    def run(self):
        print(f"MergePart: run {self.part_id}")
        with self.session as session:
            # > get the part and update timestamp to tag for 'MERGE'
            pt: Part = session.get_one(Part, self.part_id)
            pt.timestamp = time.time()
            session.commit()

            # > output directory
            mrg_path: Path = self._path.joinpath("result", "part", pt.name)
            if not mrg_path.exists():
                mrg_path.mkdir(parents=True)

            # > loop over all observables
            if "histograms_single_file" in self.config["run"]:  # @todo
                raise NotImplementedError("MergePart: histograms_single_file")
            for obs in self.config["run"]["histograms"]:
                out_file: Path = mrg_path / f"{obs}.dat"
                in_files: list[Path] = []
                # > collect histograms from all jobs
                for job in session.scalars(self.select_job):
                    if not job.rel_path:
                        continue  # @todo raise warning in logger?
                    job_path: Path = self._path / job.rel_path
                    exe_data = ExeData(job_path)
                    for out in filter(
                        lambda x: re.match(r"^.*\.{}\.s[0-9]+\.dat".format(obs), x),
                        exe_data["output_files"],
                    ):
                        in_files.append(job_path / out)
                # > merge @todo: properly call merge
                nx: int = 0 if obs == "obs" else 3
                container = NNLOJETContainer(size=len(in_files))
                for in_file in in_files:
                    try:
                        container.append(NNLOJETHistogram(nx=nx, filename=in_file))
                    except ValueError as e:
                        print(e)
                        print("error reading file:", in_file)
                container.mask_outliers(3.5, 0.01)
                container.optimise_k(maxdev_unwgt=None, nsteps=3, maxdev_steps=0.5)
                hist = container.merge(weighted=True)
                hist.write_to_file(out_file)

                #@todo keep track of a "settings.json" for merge settings used?
                # with open(out_file, "w") as out:
                #     for in_file in in_files:
                #         out.write(str(in_file))

            # > mark done
            for job in session.scalars(self.select_job):
                job.status = JobStatus.MERGED
            pt.result = -1.0
            pt.error = -1.0
            session.commit()


class MergeAll(DBMerge):
    # > merge all `Part` objects

    # @ todo add flag to skip requirements and just merge all parts

    @property
    def select_part(self):
        return select(Part).where(Part.active.is_(True))

    def requires(self):
        with self.session as session:
            return [
                self.clone(cls=MergePart, part_id=pt.id) for pt in session.scalars(self.select_part)
            ]

    def complete(self) -> bool:
        return False
        with self.session as session:
            for pt in session.scalars(select(Part).where(Part.active.is_(True))):
                if pt.timestamp is None:
                    return False
            return True

    def run(self):
        print("MergeAll: run all")
        with self.session as session:
            pass
        print("MergeAll: complete all")
        self.print_job()
