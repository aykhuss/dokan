"""NNLOJET job database

module defining the job database
"""

from ._jobstatus import JobStatus

from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship


class JobDB(DeclarativeBase):
    pass


class Part(JobDB):
    __tablename__ = "part"

    id: Mapped[int] = mapped_column(primary_key=True)

    # > same structure as nnlojet.get_lumi dictionary
    name: Mapped[str]
    string: Mapped[str]
    part: Mapped[str]
    part_num: Mapped[int]
    region: Mapped[str | None]
    order: Mapped[int]

    # @todo: future features
    # variation: Mapped[int]  # <- to identify different setups (rewgt, etc)
    active: Mapped[bool]

    # > result of the last merge
    result: Mapped[float | None]
    error: Mapped[float | None]
    # number of jobs?

    # > all jobs associated with this part
    jobs: Mapped[list["Job"]] = relationship(back_populates="part")

    def __repr__(self) -> str:
        return f"Part(id={self.id!r}, name={self.name!r}, string={self.string!r}, part={self.part!r}, part_num={self.part_num!r}, region={self.region!r}, order={self.order!r}, active={self.active!r}, result={self.result!r}, error={self.error!r})"


class Job(JobDB):
    __tablename__ = "job"

    id: Mapped[int] = mapped_column(primary_key=True)

    # > every job is associated with a `Part`
    part_id: Mapped[int] = mapped_column(ForeignKey("part.id"))
    part: Mapped["Part"] = relationship(back_populates="jobs")

    status: Mapped[int]
    # > zero: not started, > 0 : current job, < 0: past merged job
    timestamp: Mapped[float]
    path: Mapped[str | None]  # relative path to the job directory (set by DBRunner)

    # >
    mode: Mapped[int]  # [warmup|production]
    policy: Mapped[int]  # how to run
    seed: Mapped[int | None] # set in DBDispatch
    ncall: Mapped[int | None] # set before DBDispatch or auto-determined in DBRunner
    nit: Mapped[int | None] # set before DBDispatch or auto-determined in DBRunner
    elapsed_time: Mapped[float | None]
    result: Mapped[float | None]
    error: Mapped[float | None]
    chi2dof: Mapped[float | None]

    def __repr__(self) -> str:
        return f"Job(id={self.id!r}, part_id={self.part_id!r}, seed={self.seed!r}, name={self.name!r}, status={JobStatus(self.status)!r}, result={self.result!r})"
