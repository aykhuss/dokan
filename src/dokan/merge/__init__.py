"""dokan merge subpackage

DB-free statistical merge of NNLOJET histogram results.  Houses the shared
merge core (`MergeObs` and the `.dat`/HDF5 helpers) used by both the dokan
workflow (`dokan.db._dbmerge`) and the standalone `nnlojet-merge` tool.
"""

from ._core import (
    BinMask,
    MergeObs,
    build_obs_group,
)

__all__ = [
    "BinMask",
    "MergeObs",
    "build_obs_group",
]
