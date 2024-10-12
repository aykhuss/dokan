from enum import IntEnum, unique


@unique
class JobStatus(IntEnum):
    """possible jobs states in the database"""

    QUEUED = 0
    DISPATCHED = 1
    RUNNING = 2
    DONE = 3
    MERGED = 4
    FAILED = -1

    def __str__(self):
        return self.name.lower()

    def __repr__(self):
        return str(self)

    def terminated(self) -> bool:
        return self in [JobStatus.DONE, JobStatus.MERGED, JobStatus.FAILED]
