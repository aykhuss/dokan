"""Type declarations for dokan

Attributes
----------
GenericPath : TypeAlias
    a generic type for a path that can be either a string or a PathLike object
JobSize : dataclass
    the validated statistics (`ncall` x `niter`) of a single job
"""

from dataclasses import dataclass
from os import PathLike
from typing import AnyStr, TypeAlias

GenericPath: TypeAlias = AnyStr | PathLike[AnyStr]


@dataclass(frozen=True)
class JobSize:
    """Validated statistics for one job."""

    ncall: int
    niter: int

    def __post_init__(self) -> None:
        if self.ncall <= 0:
            raise ValueError(f"ncall must be positive, got {self.ncall}")
        if self.niter <= 0:
            raise ValueError(f"niter must be positive, got {self.niter}")

    @property
    def ntot(self) -> int:
        return self.ncall * self.niter
