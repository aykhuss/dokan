"""the dokan monitor task

the monitor is a live-updating table that shows the status of all jobs
and the logs that are being generated by the dokan workflow.
"""

import datetime
import time
from operator import itemgetter

from rich import box
from rich.live import Live
from rich.style import Style
from rich.table import Column, Table
from sqlalchemy import select
from sqlalchemy.orm import Session

from .db import DBTask, Log, Part
from .db._jobstatus import JobStatus
from .db._loglevel import LogLevel
from .exe import ExecutionMode


class Monitor(DBTask):
    # @todo: poll_rate? --> config

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print(f"Monitor::init  {time.ctime(self.run_tag)}")

        self._log_id: int = 0
        with self.session as session:
            last_log = session.scalars(select(Log).order_by(Log.id.desc())).first()
            if last_log:
                print(f"Monitor::init:  last log: {last_log!r}")
                self._log_id = last_log.id
                # > last run was successful: reset log table.
                if last_log.level in [LogLevel.SIG_COMP]:
                    print("Monitor::init:  clearing old logs...")
                    for log in session.scalars(select(Log)):
                        session.delete(log)
                    session.commit()

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

        self.cross_line: str = "[blue]cross = ... (waiting for first update) [/blue]"
        self.cross_time: float = time.time()

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

    def _generate_table(self, session: Session) -> Table:
        # > collect data from DB
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
            # @todo actually put in the numbrs & # of remaining jobs & current estimate for error
            title=f"[{dt_str}]\n{self.cross_line}\n(updated {datetime.timedelta(seconds=int(time.time() - self.cross_time))!s} ago)\n"
            + "[dim]legend:[/dim] [yellow][b]A[/b]ctive[/yellow] [green][b]D[/b]one[/green] [red][b]F[/b]ailed[/red]",
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

        with self.session as session:
            self._logger(session, "Monitor::run:  switching on the job status board...")

            with Live(self._generate_table(session), auto_refresh=False) as live:
                while True:
                    live.update(self._generate_table(session), refresh=True)

                    for log in session.scalars(
                        select(Log).where(Log.id > self._log_id).order_by(Log.id.asc())
                    ):
                        self._log_id = log.id  # save last id
                        if log.level == LogLevel.SIG_UPDXS:
                            self.cross_line = log.message
                            self.cross_time = log.timestamp
                            continue
                        dt_str: str = datetime.datetime.fromtimestamp(log.timestamp).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        live.console.print(
                            f"[dim][{dt_str}][/dim]({LogLevel(log.level)!r}): {log.message}"
                        )
                        if log.level in [LogLevel.SIG_COMP, LogLevel.SIG_TERM]:
                            return
                        # time.sleep(0.01)

                    time.sleep(1.0)
