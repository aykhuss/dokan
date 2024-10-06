# SPDX-FileCopyrightText: © 2024-present NNLOJET
#
# SPDX-License-Identifier: MIT

"""NNLOJET execution

module that defines all the different ways of executing NNLOJET
(platforms, modes, ...)
"""

from ._exe import ExecutionPolicy, ExecutionMode, Executor
from ._local import LocalExec

__all__ = ["ExecutionPolicy", "ExecutionMode", "Executor", "LocalExec"]
