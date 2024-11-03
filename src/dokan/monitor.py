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
        self._data[0][0] = "#"
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
        n_running: list[int] = [0, 0]
        n_success: list[int] = [0, 0]
        n_failed: list[int] = [0, 0]
        for job in pt.jobs:
            if job.mode != display_mode:
                continue
            idx: int = 0 if job.run_tag != self.run_tag else 1
            if job.status in JobStatus.success_list():
                n_success[idx] += 1
            if job.status in JobStatus.active_list():
                n_active[idx] += 1
            if job.status == JobStatus.FAILED:
                n_failed[idx] += 1
            if job.status == JobStatus.RUNNING:
                n_running[idx] += 1
        result: str = (
            "[blue]WRM[/blue]" if display_mode == ExecutionMode.WARMUP else "[magenta]PRD[/magenta]"
        )
        if n_running[1] > 0:
            result = f"[bold]{result}[/bold]"
        else:
            result = f"[dim]{result}[/dim]"
        result += f" [yellow]A[[dim]{n_active[0]}" + (
            f"+[/dim]{n_active[1]}][/yellow]" if n_active[1] > 0 else "[/dim]][/yellow]"
        )
        result += f" [green]D[[dim]{n_success[0]}" + (
            f"+[/dim]{n_success[1]}][/green]" if n_success[1] > 0 else "[/dim]][/green]"
        )
        if any(n > 0 for n in n_failed):
            result += f" [red]F[[dim]{n_failed[0]}" + (
                f"+[/dim]{n_failed[1]}][/red]" if n_failed[1] > 0 else "[/dim]][/red]"
            )
        return result

    def generate_table(self) -> Table:
        # > collect data from DB
        with self.session as session:
            for pt in session.scalars(select(Part).where(Part.active.is_(True))):
                irow: int = pt.part_num
                icol: int = self._map_col[pt.part]
                self._data[irow][icol] = self.job_summary(pt)

        # > create the table structure
        dt_str: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
            title=f"[{dt_str}](monitor): ",
            title_justify="left",
            title_style=Style(bold=False, italic=False),
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
        self.logger(f"Monitor: switching on the job status board...")
        with Live(self.generate_table()) as live:
            while True:
                live.update(self.generate_table())
                with self.session as session:
                    logs = session.execute(
                        delete(Log).returning(Log.timestamp, Log.level, Log.message)
                    ).fetchall()
                    session.commit()

                for log in logs:
                    dt_str: str = datetime.datetime.fromtimestamp(log[0]).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    live.console.print(f"[dim][{dt_str}][/dim]({LogLevel(log[1])!r}): {log[2]}")
                    if log[1] == LogLevel.SIG_TERM:
                        return
                    # time.sleep(0.01)

                time.sleep(1)
