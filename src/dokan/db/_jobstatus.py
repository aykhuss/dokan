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

    @staticmethod
    def terminated_list():
        return [JobStatus.DONE, JobStatus.MERGED, JobStatus.FAILED]

    @staticmethod
    def active_list():
        return [JobStatus.QUEUED, JobStatus.DISPATCHED, JobStatus.RUNNING]

    def terminated(self) -> bool:
        return self in JobStatus.terminated_list()

    def active(self) -> bool:
        return self in JobStatus.active_list()
