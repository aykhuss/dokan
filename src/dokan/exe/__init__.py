"""NNLOJET execution

module that defines all the different ways of executing NNLOJET
(platforms, modes, ...)
"""

from ._exe_data import ExeData
from ._exe import ExecutionPolicy, ExecutionMode, Executor
from ._local import LocalExec

__all__ = ["ExeData", "ExecutionPolicy", "ExecutionMode", "Executor", "LocalExec"]
