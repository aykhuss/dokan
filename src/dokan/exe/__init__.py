"""NNLOJET execution

module that defines all the different ways of executing NNLOJET
(platforms, modes, ...)
"""

from ._exe_config import ExecutionPolicy, ExecutionMode
from ._exe_data import ExeData
from ._executor import Executor

__all__ = ["ExeData", "ExecutionPolicy", "ExecutionMode", "Executor"]
