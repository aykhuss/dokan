import time

import luigi
from sqlalchemy import select

from ..order import Order
from ._dbtask import DBTask
from ._sqla import DokanDB, DokanLog, Part


class DBInit(DBTask):
    """initialization of the databases

    create databases if they do not exist yet. populate the `parts` table
    with the channels information and set the `active` state according to
    the requested order.

    Attributes
    ----------
    order : int
        the order of the calculation according to the `Order` IntEnum
    channels : dict
        channel description as parsed from the `NNLOJER -listlumi <PROC>` output

    """

    order: int = luigi.IntParameter(default=Order.NNLO)
    channels: dict = luigi.DictParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # > init shall always run the setup block of the DB
        self.db_setup = True
        # > create the tables if they do not exist yet
        DokanDB.metadata.create_all(self._create_engine(self.dbname))
        DokanLog.metadata.create_all(self._create_engine(self.logname))

    def complete(self) -> bool:
        with self.session as session:
            for pt in self.channels:
                stmt = select(Part).where(Part.name == pt)
                # stmt = select(Part).where(Part.name == pt).exists()
                if not session.scalars(stmt).first():
                    return False
                for db_pt in session.scalars(stmt):
                    if db_pt.active != Order(db_pt.order).is_in(Order(self.order)):
                        return False
        return True

    def run(self) -> None:
        with self.session as session:
            # self._logger(session, f"DBInit::run order = {Order(self.order)!r}")
            for db_pt in session.scalars(select(Part)):
                db_pt.active = False  # reset to be safe
            for pt in self.channels:
                stmt = select(Part).where(Part.name == pt)
                # @ todo catch case where it's already there and check it has same entries?
                db_pt = session.scalars(stmt).first()
                active: bool = Order(self.channels[pt].get("order")).is_in(Order(self.order))
                if not db_pt:
                    session.add(
                        Part(name=pt, active=active, timestamp=time.time(), **self.channels[pt])
                    )
                else:
                    db_pt.active = active
            self._safe_commit(session)
