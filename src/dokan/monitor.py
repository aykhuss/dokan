import time
import random

from rich.style import Style
from rich.live import Live
from rich.table import Table
from rich import box

from sqlalchemy import select

from .db import Part, Job, DBTask


class Monitor(DBTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print(f"Monitor:init {time.ctime(self.run_tag)}")
        self._table = {}

    def generate_table(self) -> Table:
        table = Table(box=box.ROUNDED, safe_box=False)
        table.add_column(
            "id",
            style=Style(dim=True),
            header_style=Style(bold=False, italic=True),
            justify="center",
        )
        table.add_column("V", header_style=Style(bold=True, italic=False), justify="center")
        table.add_column("R", header_style=Style(bold=True, italic=False), justify="center")

        with self.session as session:
            for pt in session.scalars(select(Part).where(Part.active.is_(True))):
                display_mode, n_active, n_success, n_failed = pt.job_summary()
                table.add_row(
                    f"{pt.part_num}",
                    f"{pt.part}",
                    f"{n_active}|{n_success}|{n_failed}",
                )

        return table

    def complete(self) -> bool:
        return False

    def run(self):
        print(f"Monitor::run")
        with Live(self.generate_table(), auto_refresh=False) as live:
            for i in range(40):
                live.console.print(f"Monitor: {i}")
                time.sleep(1)
                live.update(self.generate_table(), refresh=True)
        print("Monitor: done")
