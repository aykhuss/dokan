import time

import luigi
from sqlalchemy import select

from .db import DBTask, MergeAll, Part
from .db._dbdispatch import DBDispatch
from .db._dbmerge import MergeFinal
from .db._dbresurrect import DBResurrect
from .db._loglevel import LogLevel
from .db._sqla import Log
from .preproduction import PreProduction


class Entry(DBTask):
    resurrect: list = luigi.ListParameter(default=[])

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        with self.session as session:
            self._debug(session, f"Entry::init {time.ctime(self.run_tag)}")

    def requires(self):
        return []

    def output(self):
        return []

    def complete(self) -> bool:
        with self.session as session:
            last_log = session.scalars(select(Log).order_by(Log.id.desc())).first()
            if last_log and last_log.level in [LogLevel.SIG_COMP]:
                return True
        return False

    def run(self):
        if self.complete():
            return

        # > all pre-productions must complete before we can dispatch production jobs
        preprods: list[PreProduction] = []
        with self.session as session:
            self._debug(session, "Entry::run")
            for pt in session.scalars(select(Part).where(Part.active.is_(True))):
                # self.debug(str(pt))
                preprod = self.clone(
                    cls=PreProduction,
                    part_id=pt.id,
                )
                preprods.append(preprod)

            self._logger(session, "Entry: yield preprods")
            yield preprods
            self._logger(session, "Entry: complete preprods -> MergeAll")
            yield self.clone(MergeAll, force=True)
            self._logger(session, "Entry: complete MergeAll -> dispatch")
            # self.print_job()
            n_dispatch: int = max(len(preprods), self.config["run"]["jobs_max_concurrent"])
            dispatch: list[luigi.Task] = [
                self.clone(DBDispatch, id=0, _n=n) for n in range(n_dispatch)
            ]
            dispatch[0]._repopulate(session)
            if self.resurrect:
                dispatch = [
                    self.clone(DBResurrect, run_tag=r[0], rel_path=r[1]) for r in self.resurrect
                ] + dispatch
            self._debug(session, "Entry: yield dispatch")
            yield dispatch
            self._logger(session, "Entry: complete dispatch -> MergeFinal")
            yield self.clone(MergeFinal, force=True)
