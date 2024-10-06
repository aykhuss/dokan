"""NNLOJET job database

module that handles everything to do with the job database
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
    part_id: Mapped[int]
    region: Mapped[str | None]
    # > result of the last merge
    result: Mapped[float | None]
    error: Mapped[float | None]
    # number of jobs?
    # > all jobs associated with this part
    jobs: Mapped[list["Job"]] = relationship(back_populates="part")

    def __repr__(self) -> str:
        return f"Part(id={self.id!r}, name={self.name!r}, part={self.part!r}, part_id={self.part_id!r}, region={self.region!r}, result={self.result!r}, error={self.error!r})"


class Job(JobDB):
    __tablename__ = "job"
    id: Mapped[int] = mapped_column(primary_key=True)
    seed: Mapped[int | None]
    name: Mapped[str]
    status: Mapped[int]
    result: Mapped[float | None]
    part_id: Mapped[int] = mapped_column(ForeignKey("part.id"))
    part: Mapped["Part"] = relationship(back_populates="jobs")

    def __repr__(self) -> str:
        return f"Job(id={self.id!r}, part_id={self.part_id!r}, seed={self.seed!r}, name={self.name!r}, status={JobStatus(self.status)!r}, result={self.result!r})"
