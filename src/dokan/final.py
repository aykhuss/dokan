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
        self.debug(f"Final::init {time.ctime(self.run_tag)}")
        self.result = float("nan")
        self.error = float("inf")

    def requires(self):
        return [self.clone(MergeAll, force=True)]

    def output(self):
        return []

    def complete(self) -> bool:
        self.debug("Final::complete")
        with self.log_session as log_session:
            last_log = log_session.scalars(select(Log).order_by(Log.id.desc())).first()
            self.debug(f"Final::complete:  last_log = {last_log!r}")
            if last_log and last_log.level in [LogLevel.SIG_COMP]:
                return True
        return False

    def run(self):
        self.debug("Final::run")

        # > shut down the monitor
        self.logger("shutting down monitor...", level=LogLevel.SIG_COMP)
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
            f"[red]cross = {self.result} +/- {self.error} [{100.*rel_acc}%][/red]",
        )

        # # > use distributed time to determine the final accuracy
        # opt_dist = self.distribute_time(1.0)
        # rel_acc: float = abs(opt_dist["tot_error"] / opt_dist["tot_result"])
        # _console.print(f"{opt_dist}")
        # _console.print(
        #     f"[red]cross = {opt_dist['tot_result']} +/- {opt_dist['tot_error']} [{100.*rel_acc}%][/red]"
        # )
