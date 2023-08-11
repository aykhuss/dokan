import luigi
import os
import shutil
import json

from dokan import CONFIG


class Task(luigi.Task):
    """A dokan task

    The main task object with mandatory attributes

    Attributes:
        config (dict): to pass down the configuration for the jobs (once task is dispatched job's CONFIG no longer available)
        dir_list (list): path *relative* to CONFIG.job_path as a list of directory names
    """

    config: dict = luigi.DictParameter()
    dir_list: list = luigi.ListParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._path = os.path.join(self.config["job"]["path"], *self.dir_list)
        os.makedirs(self._path, exist_ok=True)

    def _local(self, *path):
        return os.path.join(self._path, *path)

