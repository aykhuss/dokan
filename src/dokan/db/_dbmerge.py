import luigi
import time

from abc import ABCMeta, abstractmethod
from sqlalchemy import select
from sqlalchemy.orm.base import state_attribute_str

from ..exe._exe_config import ExecutionMode
from ._sqla import Part, Job, JobStatus
from ._dbtask import DBTask


class DBMerge(DBTask, metaclass=ABCMeta):
    # > limit the resources on
    resources = {"DBMerge": 1}

    # @staticmethod
    # def clone_factory(orig: DBTask, id: int = 0):
    #     if id > 0:
    #         return orig.clone(cls=MergePart, part_id=id)
    #     else:
    #         return orig.clone(cls=MergeAll)

    @property
    @abstractmethod
    def select_part(self):
        return select(Part)

    def update_timestamp(self) -> None:
        with self.session as session:
            timestamp: float = time.time()
            for pt in session.scalars(self.select_part):
                pt.timestamp = timestamp
            session.commit()


class MergePart(DBMerge):
    # > merge only a specific `Part`
    part_id: int = luigi.IntParameter()

    @property
    def select_part(self):
        return select(Part).where(Part.id == self.part_id).where(Part.active.is_(True))

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
            for job in session.scalars(self.select_job):
                if job.status != JobStatus.MERGED:
                    return False
            return True

    def run(self):
        print(f"MergePart: run {self.part_id}")
        with self.session as session:
            if "histograms_single_file" in self.config["run"]:
                # > not yet implemented
                raise NotImplementedError("MergePart: histograms_single_file")
            else:
                for hist in self.config["run"]["histograms"]:
                    pass
            # > mark done
            pt = session.scalar(self.select_part)
            if pt:
                pt.result = 0.0
                pt.error = 0.0
                session.commit()


class MergeAll(DBMerge):
    # > merge all `Part` objects

    @property
    def select_part(self):
        return select(Part).where(Part.active.is_(True))

    def requires(self):
        with self.session as session:
            return [
                self.clone(cls=MergePart, part_id=pt.id) for pt in session.scalars(self.select_part)
            ]

    def complete(self) -> bool:
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
