from luigi import rpc, scheduler, worker


class WorkerSchedulerFactory:
    """The dokan scheduler factory

    This scheduler factory is almost identical to the one within luigi.
      luigi/interface.py: _WorkerSchedulerFactory
      luigi/worker.py: worker
    It's minimally adapted to allow for additional options to be passed to the scheduler.
    We do this since we want to avoid the use of a `luigi.cfg` file
    and want to use the `luigi.build` function to start the workflow.
    """

    def __init__(self, **kwargs):
        self.resources = kwargs.pop("resources", None)
        self.cache_task_completion = kwargs.pop("cache_task_completion", False)
        self.check_complete_on_run = kwargs.pop("check_complete_on_run", False)
        self.check_unfulfilled_deps = kwargs.pop("check_unfulfilled_deps", True)
        self.wait_interval = kwargs.pop("wait_interval", 1.0)
        if kwargs:
            raise RuntimeError(f"WorkerSchedulerFactory: left-over options {kwargs}")

    def create_local_scheduler(self):
        return scheduler.Scheduler(
            prune_on_get_work=True, record_task_history=False, resources=self.resources
        )

    def create_remote_scheduler(self, url):
        return rpc.RemoteScheduler(url)

    def create_worker(self, scheduler, worker_processes, assistant=False):
        return worker.Worker(
            scheduler=scheduler,
            worker_processes=worker_processes,
            assistant=assistant,
            cache_task_completion=self.cache_task_completion,
            check_complete_on_run=self.check_complete_on_run,
            check_unfulfilled_deps=self.check_unfulfilled_deps,
            wait_interval=self.wait_interval,
        )
