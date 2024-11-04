import luigi
import time
import json

from sqlalchemy import select

from dokan.db._loglevel import LogLevel

from .order import Order
from .db import Part, Job, DBTask, DBInit, MergeAll
from .db._dbtask import DBDispatch
from .preproduction import PreProduction


class Entry(DBTask):
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
        opt_dist = self.distribute_time(100.0)
        for pt in sorted(opt_dist["part"].items(), key=lambda x: x[1]["T_opt"], reverse=True):
            self.debug(f"{pt[0]}: {pt[1]}")
        self.debug(
            f"estimate = {opt_dist["tot_result"]} +/- {opt_dist["tot_error_estimate_opt"]} [{100.*opt_dist["tot_error_estimate_opt"]/opt_dist["tot_result"]}%]"
        )
        # self.print_job()
        n_dispatch: int = max(len(preprods), self.config["run"]["jobs_max_concurrent"])
        dispatch: list[DBDispatch] = [self.clone(DBDispatch, id=0, _n=n) for n in range(n_dispatch)]
        dispatch[0].repopulate()
        yield dispatch
        self.logger("Entry: complete dispatch -> run MergeAll")
        yield self.clone(MergeAll, force=True)  # , run_tag=0.0)
        self.logger("Entry: done", level=LogLevel.SIG_TERM)
