# SPDX-FileCopyrightText: © 2024-present NNLOJET
#
# SPDX-License-Identifier: MIT

from luigi import scheduler, rpc, worker


class WorkerSchedulerFactory:
    """The dokan scheduler factory

    This scheduler factory is almost identical to the one within luigi.
    It's minimally adapted to allow for `resources` to be passed to the scheduler.
    We do this since we want to avoid the use of a `luigi.cfg` file and want to use the `luigi.build` function to start the workflow.
    """

    def __init__(self, resources=None):
        self.resources = resources

    def create_local_scheduler(self):
        return scheduler.Scheduler(prune_on_get_work=True,
                                   record_task_history=False,
                                   resources=self.resources)

    def create_remote_scheduler(self, url):
        return rpc.RemoteScheduler(url)

    def create_worker(self, scheduler, worker_processes, assistant=False):
        return worker.Worker(scheduler=scheduler,
                             worker_processes=worker_processes,
                             assistant=assistant)
