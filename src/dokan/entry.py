import luigi
import time
import json
from sqlalchemy import select

from dokan.db._dbtask import DBDispatch

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
        preprods: list[PreProduction] = []
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
        print("Entry: complete preprods -> run MergeAll")
        yield self.clone(MergeAll, force=True)
        print("Entry: complete MergeAll -> distribute time")
        opt_dist = self.distribute_time(100.0)
        for pt in sorted(opt_dist["part"].items(), key=lambda x: x[1]["T_opt"], reverse=True):
            print(f"{pt[0]}: {pt[1]}")
        print(
            f"estimate = {opt_dist["tot_result"]} +/- {opt_dist["tot_error_estimate_opt"]} [{100.*opt_dist["tot_error_estimate_opt"]/opt_dist["tot_result"]}%]"
        )
        # self.print_job()
        dispatch: list[DBDispatch] = [self.clone(DBDispatch, id=0, _n=n) for n,_ in enumerate(preprods)]
        dispatch[0].repopulate()
        yield dispatch
        print("Entry: complete dispatch -> run MergeAll")
        yield self.clone(MergeAll, force=True)
