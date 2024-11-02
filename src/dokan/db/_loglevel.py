from enum import IntEnum, unique


@unique
class LogLevel(IntEnum):
    """possible log levels (c.f. logging module)"""

    SIG_TERM = -1  # signal to terminate the `Monitor`
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
