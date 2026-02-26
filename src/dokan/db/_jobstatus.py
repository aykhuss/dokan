from enum import IntEnum, unique


@unique
class JobStatus(IntEnum):
    """Possible job states in the database.

    Attributes
    ----------
    QUEUED : int
        Job is created and waiting to be dispatched (0).
    DISPATCHED : int
        Job has been sent to the execution backend (1).
    RUNNING : int
        Job is currently executing (2).
    DONE : int
        Job completed successfully (3).
    MERGED : int
        Job results have been merged into the final result (4).
    FAILED : int
        Job failed execution (-1).
    RECOVER : int
        Job is being recovered (-2).

    """

    QUEUED = 0
    DISPATCHED = 1
    RUNNING = 2
    DONE = 3
    MERGED = 4
    FAILED = -1
    RECOVER = -2

    def __str__(self) -> str:
        return self.name.lower()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}.{self.name}"

    @staticmethod
    def terminated_list() -> list["JobStatus"]:
        """Get list of terminated states (final states)."""
        return [JobStatus.DONE, JobStatus.MERGED, JobStatus.FAILED]

    @staticmethod
    def success_list() -> list["JobStatus"]:
        """Get list of successful terminated states."""
        return [JobStatus.DONE, JobStatus.MERGED]

    @staticmethod
    def active_list() -> list["JobStatus"]:
        """Get list of active states (non-terminal)."""
        return [
            JobStatus.QUEUED,
            JobStatus.DISPATCHED,
            JobStatus.RUNNING,
            JobStatus.RECOVER,
        ]

    def terminated(self) -> bool:
        """Check if the status represents a terminated state."""
        return self in JobStatus.terminated_list()

    def success(self) -> bool:
        """Check if the status represents a successful state."""
        return self in JobStatus.success_list()

    def active(self) -> bool:
        """Check if the status represents an active state."""
        return self in JobStatus.active_list()
