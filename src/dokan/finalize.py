import time
from pathlib import Path

from rich.console import Console
from sqlalchemy import select, func

from .db import DBTask, MergeAll
from .db._loglevel import LogLevel
from .db._sqla import Log, Part

from .order import Order

from .combine import NNLOJETContainer, NNLOJETHistogram


_console = Console()


class Finalize(DBTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # > output directory
        self.fin_path: Path = self._path.joinpath("result", "final")
        if not self.fin_path.exists():
            self.fin_path.mkdir(parents=True)

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

            # > create "final" files that merge parts into the different orders that are complete
            mrg_parent: Path = self._path.joinpath("result", "part")

            for out_order in Order:
                select_order = select(Part)  # no need to be active: .where(Part.active.is_(True))
                if int(out_order) < 0:
                    select_order = select_order.where(Part.order == out_order)
                else:
                    select_order = select_order.where(func.abs(Part.order) <= out_order)
                matched_parts = session.scalars(select_order).all()

                if any(pt.ntot <= 0 for pt in matched_parts):
                    _console.print(
                        f'[red]Final::run:  skipping "{out_order}" due to missing parts[/red]'
                    )
                    continue

                print(f"{out_order}: {list(map(lambda x: (x.id, x.ntot), matched_parts))}")

                in_files = dict((obs, []) for obs in self.config["run"]["histograms"].keys())
                for pt in matched_parts:
                    for obs in self.config["run"]["histograms"]:
                        in_file: Path = mrg_parent / pt.name / f"{obs}.dat"
                        if in_file.exists():
                            in_files[obs].append(str(in_file.relative_to(self._path)))
                        else:
                            raise FileNotFoundError(f"Finalize::run:  missing {in_file}")

                # > sum all parts
                for obs in self.config["run"]["histograms"]:
                    out_file: Path = self.fin_path / f"{out_order}.{obs}.dat"
                    nx: int = self.config["run"]["histograms"][obs]["nx"]
                    if len(in_files[obs]) == 0:
                        self._logger(
                            session, f"Finalize::run:  no files for {obs}", level=LogLevel.ERROR
                        )
                        continue
                    hist = NNLOJETHistogram()
                    for in_file in in_files[obs]:
                        try:
                            hist = hist + NNLOJETHistogram(nx=nx, filename=self._path / in_file)
                        except ValueError as e:
                            self._logger(
                                session,
                                f"error reading file {in_file} ({e!r})",
                                level=LogLevel.ERROR,
                            )
                    hist.write_to_file(out_file)

            # > shut down the monitor
            self._logger(session, "complete", level=LogLevel.SIG_COMP)
            time.sleep(1.5)

            # > parse merged cross section result
            mrg_all: MergeAll = self.requires()[0]
            dat_cross: Path = mrg_all.mrg_path / "cross.dat"
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
