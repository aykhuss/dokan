import json
import time

import luigi
from sqlalchemy import select

from .db import DBInit, DBTask, Job, MergeAll, Part
from .db._dbdispatch import DBDispatch
from .db._loglevel import LogLevel
from .order import Order
from .preproduction import PreProduction


class Entry(DBTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger(f"Entry::init {time.ctime(self.run_tag)}")

    def requires(self):
        return []

    def output(self):
        return []

    def complete(self) -> bool:
        # njobs_rem, T_rem = self.remainders()
        # if njobs_rem <= 0 or T_rem <= 0.0:
        #     return True
        # @todo still need to check target_acc somehow.
        return False

    def run(self):
        self.logger("Entry: run")
        # > all pre-productions must complete before we can dispatch production jobs
        preprods: list[PreProduction] = []
        with self.session as session:
            for pt in session.scalars(select(Part).where(Part.active.is_(True))):
                # self.debug(str(pt))
                preprod = self.clone(
                    cls=PreProduction,
                    part_id=pt.id,
                )
                preprods.append(preprod)
        self.logger("Entry: yield preprods")
        yield preprods
        self.logger("Entry: complete preprods -> run MergeAll")
        yield self.clone(MergeAll, force=True)
        self.logger("Entry: complete MergeAll -> distribute time")
        # self.print_job()
        n_dispatch: int = max(len(preprods), self.config["run"]["jobs_max_concurrent"])
        dispatch: list[DBDispatch] = [self.clone(DBDispatch, id=0, _n=n) for n in range(n_dispatch)]
        dispatch[0].repopulate()
        yield dispatch
        self.logger("Entry: complete dispatch -> run MergeAll")
        yield self.clone(MergeAll, force=True)  # , run_tag=0.0)
        opt_dist = self.distribute_time(1.0)
        rel_acc: float = abs(opt_dist["tot_error"] / opt_dist["tot_result"])
        self.logger(
            f"[red]cross = {opt_dist['tot_result']} +/- {opt_dist['tot_error']} [{100.*rel_acc}%][/red]",
            level=LogLevel.SIG_COMP,
        )
