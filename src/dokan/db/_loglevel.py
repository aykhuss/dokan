"""Numeric log levels and workflow control signals for the DBTask logging system."""

from enum import IntEnum, unique


@unique
class LogLevel(IntEnum):
    """Numeric log levels compatible with the standard `logging` module.

    Negative values are workflow control signals consumed by the Luigi
    task graph and the monitor; positive values are ordinary log levels.
    """

    SIG_TERM = -10  # terminate the monitor
    SIG_MERGE = -6  # request an out-of-band MergeAll
    SIG_DISPATCH_DONE = -5  # dynamic dispatch has reached a terminal state (budget or accuracy)
    SIG_SUB = -4  # new submission started
    SIG_FINI = -3  # finalize was triggered
    SIG_UPDXS = -2  # updated cross-section numbers are available
    SIG_COMP = -1  # workflow completed successfully
    NOTSET = 0
    DEBUG = 10
    INFO = 20
    WARN = 30
    ERROR = 40
    CRITICAL = 50

    def __str__(self):
        return self.name.lower()

    def __repr__(self):
        return str(self)

    @staticmethod
    def parse(s: str):
        return LogLevel[s.upper()]

    @staticmethod
    def argparse(s: str):
        """Parse a log-level string for use as an ``argparse`` type.

        Returns the `LogLevel` on success, or the raw string on failure so
        that argparse can generate a meaningful error message.
        """
        try:
            return LogLevel.parse(s)
        except KeyError:
            return s
