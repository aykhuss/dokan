import luigi
import os
import shutil
import json

from dokan import CONFIG
from luigi.parameter import ParameterVisibility


class Task(luigi.Task):
    """A dokan task

    The main task object with mandatory attributes

    Attributes:
        config (dict): to pass down the configuration for the jobs (once task is dispatched job's CONFIG no longer available)
        local_path (list): path *relative* (local) to CONFIG.job_path as a list of directory names
    """

    config: dict = luigi.DictParameter(visibility=ParameterVisibility.HIDDEN)
    local_path: list = luigi.ListParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._path = os.path.join(self.config["job"]["path"], *self.local_path)
        os.makedirs(self._path, exist_ok=True)

    def _local(self, *path):
        return os.path.join(self._path, *path)

