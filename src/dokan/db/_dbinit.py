"""Database bootstrap task for Dokan workflow metadata.

`DBInit` ensures both SQLite schemas exist and synchronizes the `part` table
from the channel map discovered for the process. Synchronization is idempotent:
re-running this task with the same inputs should produce no effective change.
"""

import time

import luigi
from sqlalchemy import select

from ..order import Order
from ._dbtask import DBTask
from ._sqla import DokanDB, DokanLog, Part


class DBInit(DBTask):
    """Initialization of the job databases.

    This task is responsible for creating the database schema (tables) if they
    do not exist and populating/updating the `parts` table based on the
    process channels and requested order.

    Attributes
    ----------
    order : int
        The target order of the calculation (e.g., LO, NLO, NNLO).
    channels : dict
        Channel definitions dictionary (name -> properties).

    """

    order: int = luigi.IntParameter(default=Order.NNLO)
    channels: dict = luigi.DictParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # > init shall always run the setup block of the DB
        self.db_setup = True
        # > create the tables if they do not exist yet
        # This is safe to do in __init__ as it is idempotent and required for
        # complete() to function.
        DokanDB.metadata.create_all(self._create_engine(self.dbname))
        DokanLog.metadata.create_all(self._create_engine(self.logname))

    def complete(self) -> bool:
        """Check if the database parts match the requested configuration."""
        with self.session as session:
            # Fetch all existing parts that are in our channel list
            stmt = select(Part).where(Part.name.in_(self.channels.keys()))
            existing_parts: dict[str, Part] = {p.name: p for p in session.scalars(stmt)}

            target_order: Order = Order(self.order)

            for name, _ in self.channels.items():
                if name not in existing_parts:
                    return False

                part = existing_parts[name]
                # Check if the active state matches the target order logic
                should_be_active = Order(part.order).is_in(target_order)
                if part.active != should_be_active:
                    return False

        return True

    def run(self) -> None:
        """Populate or update the parts table."""
        with self.session as session:
            # self._logger(session, f"DBInit::run order = {Order(self.order)!r}")

            # > Fetch all existing parts to update/insert efficiently
            stmt = select(Part)
            existing_parts: dict[str, Part] = {p.name: p for p in session.scalars(stmt)}

            # > First, deactivate everything to ensure clean state
            for p in existing_parts.values():
                p.active = False

            target_order: Order = Order(self.order)
            current_time: float = time.time()

            for name, channel_info in self.channels.items():
                channel_order: Order = Order(channel_info.get("order"))
                is_active: bool = channel_order.is_in(target_order)

                if name in existing_parts:
                    # > Update existing part
                    part = existing_parts[name]
                    part.active = is_active
                else:
                    # > Insert new part
                    new_part = Part(
                        name=name, active=is_active, timestamp=current_time, **channel_info
                    )
                    session.add(new_part)
                    existing_parts[name] = new_part

            self._safe_commit(session)
