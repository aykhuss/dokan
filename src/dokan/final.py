import time
from pathlib import Path

from rich.console import Console
from sqlalchemy import select

from .db import DBTask, MergeAll
from .db._loglevel import LogLevel
from .db._sqla import Log

_console = Console()


class Final(DBTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        with self.session as session:
            self._debug(session, f"Final::init {time.ctime(self.run_tag)}")
        self.result = float("nan")
        self.error = float("inf")

    def requires(self):
        return [self.clone(MergeAll, force=True)]

    def output(self):
        return []

    def complete(self) -> bool:
        with self.session as session:
            self._debug(session, "Final::complete")
            last_log = session.scalars(
                select(Log).where(Log.level < 0).order_by(Log.id.desc())
            ).first()
            self._debug(session, f"Final::complete:  last_log = {last_log!r}")
            if last_log and last_log.level in [LogLevel.SIG_COMP]:
                return True
        return False

    def run(self):
        with self.session as session:
            self._debug(session, "Final::run")

            # > shut down the monitor
            self._logger(session, "complete", level=LogLevel.SIG_COMP)
            time.sleep(1.5)

            # > parse final cross section result
            fin_mrg: MergeAll = self.requires()[0]
            dat_cross: Path = fin_mrg.fin_path / "cross.dat"
            with open(dat_cross, "rt") as cross:
                for line in cross:
                    if line.startswith("#"):
                        continue
                    self.result = float(line.split()[0])
                    self.error = float(line.split()[1])
                    break
            rel_acc: float = abs(self.error / self.result)
            _console.print(
                f"\n[blue]cross = ({self.result} +/- {self.error}) fb  [{rel_acc * 1e2:.3}%][/blue]\n"
            )

            # > use `distribute_time` to fetch optimization target
            # > & time estimate to reach desired accuracy
            opt_dist = self._distribute_time(session, 0.0)
            # _console.print(f"{opt_dist}")
            opt_target: str = (
                self.config["run"]["opt_target"]
                if "opt_target" in self.config["run"]
                else "cross_hist"  # default
            )
            _console.print(
                f'option "[bold]{opt_target}[/bold]" chosen to target optimization of rel. acc.'
            )
            rel_acc: float = abs(opt_dist["tot_error"] / opt_dist["tot_result"])
            if rel_acc <= self.config["run"]["target_rel_acc"] * (1.05):
                _console.print(
                    f"[green]reached rel. acc. {rel_acc * 1e2:.3}% on {opt_target}[/green] "
                    + f"(requested: {self.config['run']['target_rel_acc'] * 1e2:.3}%)"
                )
            else:
                _console.print(
                    f"[red]reached rel. acc. {rel_acc * 1e2:.3}% on {opt_target}[/red] "
                    + f"(requested: {self.config['run']['target_rel_acc'] * 1e2:.3}%)"
                )
                njobs_target: int = (
                    round(opt_dist["T_target"] / self.config["run"]["job_max_runtime"]) + 1
                )
                _console.print(
                    f"still need about [bold]{njobs_target}[/bold] jobs [dim](run time: {self.config['run']['job_max_runtime']}s)[/dim] to reach desired target accuracy."
                )
