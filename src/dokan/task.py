"""Task class within the dokan workflow

sub-class of a luigi Task to impose mandatory attributes to a workflow task.
"""

import signal
import sys
from pathlib import Path
from typing import NoReturn

import luigi
from luigi.parameter import ParameterVisibility

from ._types import GenericPath


class Task(luigi.Task):
    """A dokan Task

    The main Task object in dokan with mandatory attributes

    Attributes
    ----------
    config : dict
        pass down the configuration for the jobs.
        Needed because once a Task is dispatched, global CONFIG is no longer
        available. Also facilitates the possibility of overrides that propagate
        down stream.
    local_path : list[str]
        path *relative* (local) to CONFIG.job_path as a list of directory names
    """

    config: dict = luigi.DictParameter(visibility=ParameterVisibility.HIDDEN)
    local_path: list[str] = luigi.ListParameter(default=[])

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._path: Path = Path(self.config["run"]["path"]).joinpath(*self.local_path)
        self._path.mkdir(parents=True, exist_ok=True)
        # > signal handlers at the level of the workflow tasks
        signal.signal(signal.SIGINT, self.graceful_exit)
        signal.signal(signal.SIGTERM, self.graceful_exit)

    # @todo: maybe add a _post_init_ routine that can be overwritten on a task basis?

    def _local(self, *path: GenericPath) -> Path:
        """get the "Task local" path

        take path and append to local path of the dokan task.

        Parameters
        ----------
        path : Tuple[str]
            list of paths (directories, filename) that will be concatenated.

        Returns
        -------
        Path
            resultant path relative to the task
        """
        return self._path.joinpath(*path)

    def graceful_exit(self, sig, frame) -> NoReturn | None:
        sys.exit(0)
