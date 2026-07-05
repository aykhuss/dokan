"""Live monitor task for Dokan workflow status and logs.

The monitor renders a Rich table with per-part job summaries and streams log
records from the DB in near real-time.
"""

import datetime
import time
from typing import NamedTuple

from rich import box
from rich.console import Console
from rich.live import Live
from rich.style import Style
from rich.table import Column, Table
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .db import DBTask, Log, Part
from .db._jobstatus import JobStatus
from .db._loglevel import LogLevel
from .db._sqla import Job
from .exe import ExecutionMode

_console = Console()


def _part_label(pt: Part) -> str:
    """Column label of a part: name plus region suffix (e.g. "RRa")."""
    return pt.part + (pt.region or "")


class _JobCount(NamedTuple):
    """One aggregated `GROUP BY` row: #jobs of a part in a (mode, status) bucket."""

    mode: ExecutionMode
    status: JobStatus
    current: bool  # job belongs to the current run tag
    n: int


class Monitor(DBTask):
    """Render and refresh the live status board.

    Notes
    -----
    - This task is intentionally long-lived and exits only on completion or
      termination signals in the log stream.
    - Completion is signal-driven (`SIG_COMP` / `SIG_TERM`), not output-target
      driven, so `complete()` is always False.
    """

    # @todo: poll_rate? --> config

    def __init__(self, *args, **kwargs):
        """Initialize static monitor state (no DB access at construction time)."""
        super().__init__(*args, **kwargs)
        self._refresh_delay = self.config["ui"].get("refresh_delay", 1.5)
        self._log_id: int = 0
        self.cross_line: str = "[blue]cross = ... (waiting for first update) [/blue]"
        self.cross_time: float = time.time()

    def _init_board(self, session: Session) -> None:
        """Query the DB once to build the static table layout and the log cursor.

        The layout is frozen for the lifetime of the monitor: parts activated
        after this point are not displayed.
        """
        last_log = session.scalars(select(Log).order_by(Log.id.desc())).first()
        if last_log:
            self._log_id = last_log.id

        parts: list[Part] = list(session.scalars(select(Part).where(Part.active.is_(True))))
        nchan: int = max((pt.part_num for pt in parts), default=0)  # maximum # of partonic channels
        # > columns: unique part labels sorted by order, then alphabetically
        part_order: list[tuple[int, str]] = sorted({(abs(pt.order), _part_label(pt)) for pt in parts})
        map_col: dict[str, int] = {label: icol for icol, (_, label) in enumerate(part_order, start=1)}
        # > static (row, column) cell of each active part
        self._cells: dict[int, tuple[int, int]] = {
            pt.id: (pt.part_num, map_col[_part_label(pt)]) for pt in parts
        }
        self._data: list[list[str]] = [["-" for _ in range(len(part_order) + 1)] for _ in range(nchan + 1)]
        self._data[0][0] = "#"
        for irow in range(1, len(self._data)):
            self._data[irow][0] = f"{irow}"
        for pt_name, icol in map_col.items():
            self._data[0][icol] = pt_name

    def _collect_job_counts(self, session: Session) -> dict[int, list[_JobCount]]:
        """One aggregate query per refresh: per-part (mode, status, is-current-run, count).

        Replaces lazily loading every `Job` row of every part through the ORM
        relationship on each refresh — the row count scales with the number of
        distinct (part, mode, status) combinations, not with the number of jobs.
        """
        is_current = (Job.run_tag == self.run_tag).label("current")
        rows = session.execute(
            select(Job.part_id, Job.mode, Job.status, is_current, func.count(Job.id))
            .join(Part, Job.part_id == Part.id)
            .where(Part.active.is_(True))
            .group_by(Job.part_id, Job.mode, Job.status, is_current)
        ).all()
        counts: dict[int, list[_JobCount]] = {}
        for part_id, mode, status, current, n in rows:
            counts.setdefault(part_id, []).append(
                _JobCount(ExecutionMode(mode), JobStatus(status), bool(current), n)
            )
        return counts

    def job_summary(self, part_counts: list[_JobCount]) -> str:
        """Build one compact status string for a single active part from its counts."""
        display_mode: ExecutionMode = (
            ExecutionMode.WARMUP
            if any(
                c.mode == ExecutionMode.WARMUP and c.status in JobStatus.active_list()
                for c in part_counts
            )
            else ExecutionMode.PRODUCTION
        )
        # > only "running" needs the current-run split (drives the bold highlight);
        # > all other counters are displayed as totals across run tags
        n_active = n_success = n_failed = n_running_current = 0
        for c in part_counts:
            if c.mode != display_mode:
                continue
            if c.status in JobStatus.success_list():
                n_success += c.n
            if c.status in JobStatus.active_list():
                n_active += c.n
            if c.status == JobStatus.FAILED:
                n_failed += c.n
            if c.status == JobStatus.RUNNING and c.current:
                n_running_current += c.n
        result: str = "[blue]WRM[/blue]" if display_mode == ExecutionMode.WARMUP else "[magenta]PRD[/magenta]"
        result = f"[bold]{result}[/bold]" if n_running_current > 0 else f"[dim]{result}[/dim]"
        result += f" [yellow]A[dim][{n_running_current}/{n_active}][/dim][/yellow]"
        result += f" [green]D[dim][{n_success}][/dim][/green]"
        if n_failed > 0:
            result += f" [red]F[dim][{n_failed}][/dim][/red]"
        return result

    def _generate_table(self, session: Session) -> Table:
        """Generate the current table snapshot from DB state."""
        # > one aggregate query for all parts; cell positions are static from `_init_board`
        counts = self._collect_job_counts(session)
        for part_id, (irow, icol) in self._cells.items():
            self._data[irow][icol] = self.job_summary(counts.get(part_id, []))

        # > create the table structure
        dt_str: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        table: Table = Table(
            Column(
                self._data[0][0],
                style=Style(dim=True),
                header_style=Style(bold=False, italic=False, dim=True),
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
            title=f"[{dt_str}]\n{self.cross_line}\n"
            + f"(updated {datetime.timedelta(seconds=int(time.time() - self.cross_time))!s} ago)\n"
            + "[dim]legend:[/dim]"
            + " [yellow][b]A[/b]ctive[/yellow] [green][b]D[/b]one[/green] [red][b]F[/b]ailed[/red]",
            title_justify="left",
            title_style=Style(bold=False, italic=False),
        )
        # > populate with data
        for irow in range(1, len(self._data)):
            table.add_row(*self._data[irow])

        return table

    def complete(self) -> bool:
        """Always return False; monitor lifetime is controlled inside `run()`."""
        return False

    def run(self):
        """Start the live monitor loop and stream logs until termination signal."""
        if not self.config["ui"]["monitor"]:
            return

        _console.print(f"Monitor::run:  {time.ctime(self.run_tag)}")
        with self.session as session:
            self._init_board(session)
            self._logger(session, "Monitor::run:  switching on the job status board...")
            initial_table = self._generate_table(session)

        with Live(initial_table, auto_refresh=False) as live:
            while True:
                with self.session as session:
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
                        live.console.print(f"[dim][{dt_str}][/dim]({LogLevel(log.level)!r}): {log.message}")
                        if log.level in [LogLevel.SIG_COMP, LogLevel.SIG_TERM]:
                            return
                        # time.sleep(0.01)

                time.sleep(self._refresh_delay)
