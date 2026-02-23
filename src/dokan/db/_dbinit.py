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
    channels : dict
        Full channel definition map (`name -> channel metadata`) from process
        discovery.
    order : int
        The target order of the calculation (e.g., LO, NLO, NNLO).
    select_channels : list
        Optional explicit channel-name allowlist. When non-empty, this takes
        precedence over `order`.

    """

    channels: dict = luigi.DictParameter()  # all channels
    order: int = luigi.IntParameter(default=Order.NNLO)
    # if select_channels is non-empty, we ignore the order
    select_channels: list[str] = luigi.ListParameter(default=[])

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger_prefix: str = self.__class__.__name__
        # > init shall always run the setup block of the DB
        self.db_setup = True
        # > create the tables if they do not exist yet
        # This is safe to do in __init__ as it is idempotent and required for
        # complete() to function.
        DokanDB.metadata.create_all(self._create_engine(self.dbname))
        DokanLog.metadata.create_all(self._create_engine(self.logname))
        # > determine the channels that should be activated
        self.activate_channels: list[str] = self._resolve_activate_channels()

    def _resolve_activate_channels(self) -> list[str]:
        """Build the normalized activation list for this DBInit instance.

        Raises
        ------
        ValueError
            If `select_channels` contains unknown names or if a channel has an
            invalid/missing `order` field.

        """
        if self.select_channels:
            unknown = [name for name in self.select_channels if name not in self.channels]
            if unknown:
                raise ValueError(
                    f"{self._logger_prefix}::init: select_channels contains unknown entries: {unknown}"
                )
            return self.select_channels

        target_order: Order = Order(self.order)
        activate: list[str] = []
        for name, channel_info in self.channels.items():
            if "order" not in channel_info:
                raise ValueError(
                    f"{self._logger_prefix}::init: channel {name!r} has no 'order' entry"
                )
            channel_order = Order(channel_info["order"])
            if channel_order.is_in(target_order):
                activate.append(name)
        return activate

    def complete(self) -> bool:
        """Check if DB activation flags match the requested channel selection."""
        with self.session as session:
            # > Fetch all parts in the database
            db_parts: dict[str, Part] = {p.name: p for p in session.scalars(select(Part))}

            # > there are channels not yet added to the DB
            if any(ac not in db_parts for ac in self.activate_channels):
                return False

            for name, part in db_parts.items():
                if part.active != (name in self.activate_channels):
                    return False

        return True

    def run(self) -> None:
        """Synchronize `Part.active` for all known channels.

        Existing rows are deactivated first, then channels selected in
        `activate_channels` are activated (and inserted if missing).
        """
        with self.session as session:
            self._logger(session, f"{self._logger_prefix}::run activate: {self.activate_channels}")

            # > Fetch all existing parts to update/insert efficiently
            db_parts: dict[str, Part] = {p.name: p for p in session.scalars(select(Part))}

            # > First, deactivate everything to ensure clean state
            for p in db_parts.values():
                p.active = False

            current_time: float = time.time()

            for name in self.activate_channels:
                channel_info: dict = self.channels[name]

                if name in db_parts:
                    # > Update existing part
                    part = db_parts[name]
                    part.active = True
                else:
                    # > Insert new part
                    new_part = Part(name=name, active=True, timestamp=current_time, **channel_info)
                    session.add(new_part)
                    db_parts[name] = new_part

            self._safe_commit(session)
