import luigi
import time
import json
from sqlalchemy import select

from .order import Order
from .db import Part, Job, DBTask, DBInit, MergeAll
from .preproduction import PreProduction


class Entry(DBTask):
    # @todo job options
    # seed start
    # njobs max
    # max concurrent
    order: int = luigi.IntParameter(default=Order.NNLO)
    channels: dict = luigi.DictParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print(f"starter: {self.run_tag}: {time.ctime(self.run_tag)}")

    def requires(self):
        # @todo variations to be added here?
        return [
            self.clone(
                cls=DBInit,
                channels=self.channels,
            )
        ]

    def output(self):
        return []

    def complete(self) -> bool:
        return False

    def run(self):
        print("Entry: run")
        # > all pre-productions must complete before we can dispatch production jobs
        preprods = []
        with self.session as session:
            for pt in session.scalars(select(Part).where(Part.active.is_(True))):
                # print(pt)
                preprod = self.clone(
                    cls=PreProduction,
                    part_id=pt.id,
                )
                preprods.append(preprod)
        print("Entry: yield preprods")
        yield preprods
        print("Entry: complete preprods")
        yield self.clone(MergeAll, force=True)
        print("Entry: complete MergeAll")
        opt_dist_T, val, err_est = self.distribute_time(1.0)
        print(f"Entry: distribute_time {json.dumps(opt_dist_T, indent=2)}\nestimate = {val} +/- {err_est} [{100.*err_est/val}%]")
        # self.print_job()
