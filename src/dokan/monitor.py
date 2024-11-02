import luigi
import time
import datetime

from operator import itemgetter

from rich.style import Style
from rich.live import Live
from rich.table import Table, Column
from rich import box

from sqlalchemy import select, delete

from .db import Part, Job, Log, DBTask
from .db._jobstatus import JobStatus
from .db._loglevel import LogLevel
from .exe import ExecutionMode


class Monitor(DBTask):
    # @todo: poll_rate? --> config

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print(f"Monitor:init {time.ctime(self.run_tag)}")
        self._nchan: int = 0
        part_order: list[tuple[int, str]] = []
        with self.session as session:
            for pt in session.scalars(select(Part).where(Part.active.is_(True))):
                self._nchan = max(self._nchan, pt.part_num)
                ipt: tuple[int, str] = (abs(pt.order), pt.part)
                if ipt not in part_order:
                    part_order.append(ipt)
        part_order.sort(key=itemgetter(1))  # alphabetically by name
        part_order.sort(key=itemgetter(0))  # then finally by the order
        self._map_col: dict[str, int] = dict(
            (ipt[1], icol) for icol, ipt in enumerate(part_order, start=1)
        )
        self._data: list[list[str]] = [
            ["-" for _ in range(len(part_order) + 1)] for _ in range(self._nchan + 1)
        ]
        self._data[0][0] = "id"
        for irow in range(1, len(self._data)):
            self._data[irow][0] = f"{irow}"
        for pt_name, icol in self._map_col.items():
            self._data[0][icol] = pt_name

    def job_summary(self, pt: Part) -> str:
        display_mode: ExecutionMode = (
            ExecutionMode.WARMUP
            if any(
                job.mode == ExecutionMode.WARMUP
                for job in pt.jobs
                if job.status in JobStatus.active_list()
            )
            else ExecutionMode.PRODUCTION
        )
        n_active: list[int] = [0, 0]
        n_success: list[int] = [0, 0]
        n_failed: list[int] = [0, 0]
        for job in pt.jobs:
            if job.mode != display_mode:
                continue
            idx: int = 0 if job.run_tag != self.run_tag else 1
            if job.status in JobStatus.success_list():
                n_success[idx] += 1
            elif job.status in JobStatus.active_list():
                n_active[idx] += 1
            elif job.status == JobStatus.FAILED:
                n_failed[idx] += 1
        result: str = (
            "[bold blue]Warm[/bold blue]"
            if display_mode == ExecutionMode.WARMUP
            else "[bold magenta]Prod[/bold magenta]"
        )
        result += f" [yellow][bold]A[/bold][[dim]{n_active[0]}+[/dim]{n_active[1]}][/yellow]"
        result += f" [green][bold]S[/bold][[dim]{n_success[0]}+[/dim]{n_success[1]}][/green]"
        result += f" [red][bold]F[/bold][[dim]{n_failed[0]}+[/dim]{n_failed[1]}][/red]"
        return result

    def generate_table(self) -> Table:
        # > collect data from DB
        with self.session as session:
            for pt in session.scalars(select(Part).where(Part.active.is_(True))):
                irow: int = pt.part_num
                icol: int = self._map_col[pt.part]
                self._data[irow][icol] = self.job_summary(pt)

        # > create the table structure
        table: Table = Table(
            Column(
                self._data[0][0],
                style=Style(dim=True),
                header_style=Style(bold=False, italic=True, dim=True),
                justify="center",
            ),
            *(
                Column(
                    self._data[0][icol],
                    header_style=Style(bold=True, italic=False),
                    justify="center",
                )
                for icol in range(1, len(self._data[0]))
            ),
            box=box.ROUNDED,
            safe_box=False,
            title="NNLOJET [dim]-[/dim] asdf",
        )
        # > populate with data
        for irow in range(1, len(self._data)):
            table.add_row(*self._data[irow])

        return table

    def complete(self) -> bool:
        return False

    def run(self):
        if not self.config["ui"]["monitor"]:
            return
        print(f"Monitor::run")
        with Live(self.generate_table(), auto_refresh=False) as live:
            for i in range(60):
                time.sleep(1)
                live.update(self.generate_table(), refresh=True)
                with self.session as session:
                    logs = session.execute(
                        delete(Log).returning(Log.timestamp, Log.level, Log.message)
                    ).fetchall()
                    session.commit()

                for log in logs:
                    time.sleep(0.1)
                    dt_str: str = datetime.datetime.fromtimestamp(log[0]).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    live.console.print(f"[dim][{dt_str}][/dim]({LogLevel(log[1])!r}): {log[2]}")

        print("Monitor: done")
