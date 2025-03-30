"""Definition of logging levels and signals

log levels used in `DBTask.logger` and displayed in `Monitor`
as well as signals to trigger events such as the termination of the monitor.
"""

from enum import IntEnum, unique


@unique
class LogLevel(IntEnum):
    """possible log levels (c.f. logging module)"""

    SIG_TERM = -10  # signal to terminate the monitor
    SIG_SUB = -4  # signal to indicate new submission
    SIG_FINI = -3  # signal to indicate finalize was triggered
    SIG_UPDXS = -2  # signal to send an updated XS number
    SIG_COMP = -1  # signal to indicate successful completion
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
