"""NNLOJET execution interface.

Defines an abstraction to execute NNLOJET on different backends (policies)
and a factory design pattern to obtain tasks for the different policies.
"""

import logging
import time
from abc import ABCMeta, abstractmethod
from pathlib import Path
from typing import ClassVar

import luigi

from .._types import GenericPath
from ..db._loglevel import LogLevel
from ._exe_config import ExecutionPolicy
from ._exe_data import ExeData


class Executor(luigi.Task, metaclass=ABCMeta):
    """Abstract base class for NNLOJET execution tasks.

    This class handles the setup, execution, and output collection for
    NNLOJET jobs. It delegates the actual execution mechanism to
    subclasses via the `exe` method.

    Attributes
    ----------
    path : str
        Path to the execution directory.
    log_level : LogLevel
        Logging level for the task.

    """

    _file_log: str = "exe.log"

    # Filesystem scanning parameters
    FS_MAX_RETRY: ClassVar[int] = 10
    FS_DELAY: ClassVar[float] = 1.0

    path: str = luigi.Parameter()
    log_level: LogLevel = luigi.OptionalIntParameter(default=LogLevel.INFO)

    priority = 100

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exe_data: ExeData = ExeData(Path(self.path))
        # > we just use a log file to collect output
        self.file_log: Path = Path(self.path) / self._file_log

    @property
    def exe_logger(self) -> logging.Logger:
        """Lazy-initialized logger for the executor.

        Returns
        -------
        logging.Logger
            A logger instance configured to write to the execution log file.

        """
        # Create a logger specific to this executor identity to avoid handler collisions
        logger = logging.getLogger(f"dokan.executor.{self.path}")
        if not logger.handlers:
            logger.propagate = False
            logger.setLevel(logging.DEBUG)  # Filter in _logger based on self.log_level
            try:
                # Ensure directory exists before creating FileHandler
                self.file_log.parent.mkdir(parents=True, exist_ok=True)
                handler = logging.FileHandler(self.file_log, mode="a", encoding="utf-8")
                # Format matches the previous manual implementation style
                formatter = logging.Formatter(
                    "[%(asctime)s](%(levelname)s): %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
                )
                handler.setFormatter(formatter)
                logger.addHandler(handler)
            except Exception:
                # Fallback to no-op if file cannot be opened (e.g. permissions)
                pass
        return logger

    def _logger(self, message: str, level: LogLevel = LogLevel.INFO) -> None:
        """Log a message with a specific level.

        Parameters
        ----------
        message : str
            The message to log.
        level : LogLevel, optional
            The severity level of the message (default is INFO).

        """
        # > pass through log level & all signals
        if level >= 0 and level < self.log_level:
            return

        # Map IntEnum levels to logging levels
        level_val = int(level)
        if level_val < 0:
            # For signals (negative values), use INFO but include the signal name
            self.exe_logger.info(f"({level!r}): {message}")
        else:
            self.exe_logger.log(level_val, message)

    def _debug(self, message: str) -> None:
        """Log a debug message.

        Parameters
        ----------
        message : str
            The debug message.

        """
        self._logger(message, LogLevel.DEBUG)

    @staticmethod
    def get_cls(policy: ExecutionPolicy):
        """Get the Executor subclass for a given policy.

        Parameters
        ----------
        policy : ExecutionPolicy
            The execution policy (LOCAL, HTCONDOR, SLURM).

        Returns
        -------
        type
            The Executor subclass.

        Raises
        ------
        TypeError
            If the policy is invalid.

        """
        # > local import to avoid cyclic dependence
        from .htcondor import HTCondorExec
        from .local import BatchLocalExec
        from .slurm import SlurmExec

        if policy == ExecutionPolicy.LOCAL:
            return BatchLocalExec

        if policy == ExecutionPolicy.HTCONDOR:
            return HTCondorExec

        if policy == ExecutionPolicy.SLURM:
            return SlurmExec

        raise TypeError(f"invalid ExecutionPolicy: {policy!r}")

    @staticmethod
    def factory(policy: ExecutionPolicy = ExecutionPolicy.LOCAL, *args, **kwargs):
        """Create an Executor for a specific policy via the factory pattern.

        Parameters
        ----------
        policy : ExecutionPolicy, optional
            The execution policy (default is LOCAL).
        *args, **kwargs
            Arguments passed to the Executor constructor.

        Returns
        -------
        Executor
            An instance of the specific Executor subclass.

        """
        exec_cls = Executor.get_cls(policy)
        return exec_cls(*args, **kwargs)

    @staticmethod
    def templates() -> list[GenericPath]:
        """List of built-in templates for this executor.

        If the executor requires additional template files, such as submission
        files, these should be provided through this method though an override.

        Returns
        -------
        list[GenericPath]
            A list of all built-in template files for the executor.

        """
        return []

    def output(self) -> list[luigi.Target]:
        """Get the task output.

        Returns
        -------
        list[luigi.Target]
            The final status file (`job.json`) as a LocalTarget.

        """
        return [luigi.LocalTarget(self.exe_data.file_fin)]

    @abstractmethod
    def exe(self):
        """Execute the job.

        This method must be overridden by subclasses to implement the
        backend-specific execution logic.
        """
        raise NotImplementedError("Executor::exe: abstract method must be overridden!")

    def run(self):
        """Run the execution task.

        This method handles:
        1. Scanning for existing results (recovery).
        2. Setting timestamps.
        3. Invoking `exe()` if necessary.
        4. Collecting output files and finalizing the execution data.
        """
        # > more preparation for execution?

        # > scan directory and update ExeData (recovery mode)
        self.exe_data.scan_dir([self._file_log])
        if "timestamp" not in self.exe_data:
            self.exe_data["timestamp"] = time.time()
        self.exe_data.write()

        if not self.exe_data.is_complete:
            # > some systems have a different resolution in timestamps
            # > time.time() vs. os.stat().st_mtime
            # > this buffer ensures time ordering works
            time.sleep(1.0)

            # > call the backend specific execution
            try:
                self.exe()
            except Exception as e:
                self._logger(f"exception in exe: {e}", level=LogLevel.ERROR)
                raise
        else:
            self._logger("Executor::run: skipped exe()", level=LogLevel.DEBUG)

        self.exe_data.scan_dir(
            [self._file_log], fs_max_retry=self.FS_MAX_RETRY, fs_delay=self.FS_DELAY
        )
        self.exe_data.finalize()
