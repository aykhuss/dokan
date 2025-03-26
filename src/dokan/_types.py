"""Type declarations for dokan

Attributes
----------
GenericPath : TypeAlias
    a generic type for a path that can be either a string or a PathLike object
"""

from os import PathLike
from typing import AnyStr, TypeAlias

GenericPath: TypeAlias = AnyStr | PathLike[AnyStr]
