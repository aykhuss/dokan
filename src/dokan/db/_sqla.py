"""NNLOJET job database

module defining the job database
"""


import datetime

from sqlalchemy import BigInteger, ForeignKey
from sqlalchemy import String
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship

from ..exe._exe_config import ExecutionMode, ExecutionPolicy
from ._jobstatus import JobStatus


class DokanDB(DeclarativeBase):
    pass


class Part(DokanDB):
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
    active: Mapped[bool] = mapped_column(default=False)
    timestamp: Mapped[float] = mapped_column(default=0.0)

    # > result of the last merge
    Ttot: Mapped[float] = mapped_column(default=0.0)
    ntot: Mapped[float] = mapped_column(BigInteger(), default=0)
    result: Mapped[float] = mapped_column(default=0.0)
    error: Mapped[float] = mapped_column(default=float("inf"))
    # number of jobs?

    # > all jobs associated with this part
    jobs: Mapped[list["Job"]] = relationship(back_populates="part")

    def __repr__(self) -> str:
        return f"Part(id={self.id!r}, name={self.name!r}, string={self.string!r}, part={self.part!r}, part_num={self.part_num!r}, region={self.region!r}, order={self.order!r}, active={self.active!r}, result={self.result!r}, error={self.error!r})"


class Job(DokanDB):
    __tablename__ = "job"

    id: Mapped[int] = mapped_column(primary_key=True)

    # > every job is associated with a `Part`
    part_id: Mapped[int] = mapped_column(ForeignKey("part.id"))
    part: Mapped["Part"] = relationship(back_populates="jobs")

    run_tag: Mapped[float] = mapped_column(default=0.0)
    status: Mapped[int]
    timestamp: Mapped[float] = mapped_column(default=0.0)
    rel_path: Mapped[str | None]  # relative path to the job directory (set by DBRunner)

    # >
    mode: Mapped[int]  # [warmup|production]
    policy: Mapped[int]  # how to run
    seed: Mapped[int | None]  # set in DBDispatch
    # (ncall,niter) set before DBDispatch or auto-determined in DBRunner
    ncall: Mapped[int] = mapped_column(default=0)
    niter: Mapped[int] = mapped_column(default=0)
    elapsed_time: Mapped[float] = mapped_column(default=0.0)
    result: Mapped[float] = mapped_column(default=0.0)
    error: Mapped[float] = mapped_column(default=float("inf"))
    chi2dof: Mapped[float] = mapped_column(default=float("inf"))

    def __repr__(self) -> str:
        return f"Job(id={self.id!r}, part_id={self.part_id!r}, status={(self.status)!r}, timestamp={self.timestamp!r}, mode={ExecutionMode(self.mode)!r}, policy={ExecutionPolicy(self.policy)!r})"


class Log(DokanDB):
    __tablename__ = "log"

    id: Mapped[int] = mapped_column(primary_key=True)
    level: Mapped[int]
    timestamp: Mapped[float] = mapped_column(default=0.0)
    message: Mapped[str] = mapped_column(default="")

    def __repr__(self) -> str:
        return f"Log(id={self.id!r}, level={self.level!r}, timestamp={(self.timestamp)!r}, message={self.message!r})"
