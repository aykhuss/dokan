"""NNLOJET execution settings

module that collects Enum types for different settings to execute NNLOJET
(platforms, modes, ...)
"""

from enum import IntEnum, unique


@unique
class ExecutionPolicy(IntEnum):
    """policies on how/where to execute NNLOJET"""

    # NULL = 0
    LOCAL = 1
    HTCONDOR = 2
    SLURM = 3
    # LSF = 4
    # ...

    def __str__(self):
        return self.name.lower()

    def __repr__(self):
        return str(self)

    @staticmethod
    def parse(s: str):
        return ExecutionPolicy[s.upper()]

    @staticmethod
    def argparse(s: str):
        """method for `argparse`"""
        try:
            return ExecutionPolicy.parse(s)
        except KeyError:
            return s


@unique
class ExecutionMode(IntEnum):
    """The two execution modes of NNLOJET"""

    # NULL = 0
    WARMUP = 1
    PRODUCTION = 2

    def __str__(self):
        return self.name.lower()
