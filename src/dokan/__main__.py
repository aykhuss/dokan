"""The main execution of the NNLOJET workflow"""

import luigi

# from luigi.execution_summary import LuigiRunResult
import argparse
import os
import shutil
import sys
import time
import multiprocessing

from pathlib import Path
from rich.console import Console
from rich.prompt import Prompt, IntPrompt, FloatPrompt, PromptBase, Confirm
from rich.syntax import Syntax

import dokan
import dokan.nnlojet
from dokan.exe import ExecutionPolicy
from dokan.order import Order
from dokan.util import parse_time_interval


class TimeIntervalPrompt(PromptBase[float]):
    response_type = float
    validate_error_message = "[prompt.invalid]Please enter a valid time interval"

    def process_response(self, value: str) -> float:
        return parse_time_interval(value.strip())


class OrderPrompt(PromptBase[Order]):
    response_type = Order
    validate_error_message = "[prompt.invalid]Please enter a valid order"

    def process_response(self, value: str) -> Order:
        return self.response_type.argparse(value.strip())


class ExecutionPolicyPrompt(PromptBase[ExecutionPolicy]):
    response_type = ExecutionPolicy
    validate_error_message = "[prompt.invalid]Please enter a valid order"

    def process_response(self, value: str) -> ExecutionPolicy:
        return self.response_type.argparse(value.strip())


def main() -> None:
    console = Console()

    parser = argparse.ArgumentParser(description="dokan: an automated NNLOJET workflow")
    parser.add_argument("--exe", dest="exe", help="path to NNLOJET executable")
    subparsers = parser.add_subparsers(dest="action")

    # > subcommand: init
    parser_init = subparsers.add_parser("init", help="initialise a run")
    parser_init.add_argument("runcard", metavar="RUNCARD", help="NNLOJET runcard")
    parser_init.add_argument(
        "-o", "--output", dest="run_path", help="destination of the run directory"
    )

    # > subcommand: submit
    parser_submit = subparsers.add_parser("submit", help="submit a run")
    parser_submit.add_argument("run_path", metavar="RUN", help="run directory")
    parser_submit.add_argument(
        "--policy",
        type=ExecutionPolicy.argparse,
        choices=list(ExecutionPolicy),
        default=ExecutionPolicy.LOCAL,
        dest="policy",
        help="execution policy",
    )

    # > parse arguments
    args = parser.parse_args()
    if args.action is None:
        parser.print_help()
        sys.exit("please specify a subcommand")

    nnlojet_exe = None
    if args.action == "init":
        nnlojet_exe = shutil.which("NNLOJET")
    if args.exe is not None:
        path_exe: Path = Path(args.exe)
        if path_exe.is_file() and os.access(path_exe, os.X_OK):
            nnlojet_exe = str(path_exe.absolute())
        else:
            sys.exit(f"invalid executable {path_exe}")

    # >-----
    if args.action == "init":
        config: dokan.Config = dokan.Config(default_ok=True)
        runcard: dokan.Runcard = dokan.Runcard(runcard=args.runcard)
        if nnlojet_exe is None:
            prompt_exe = Prompt.ask("Could not find an NNLOJET executable. Please specify path")
            path_exe: Path = Path(prompt_exe)
            if path_exe.is_file() and os.access(path_exe, os.X_OK):
                nnlojet_exe = str(path_exe.absolute())
            else:
                sys.exit(f"invalid executable {str(path_exe.absolute())}")

        # > save all to the run config file
        if args.run_path:
            config.set_path(args.run_path)
        else:
            config.set_path(os.path.relpath(runcard.data["run_name"]))
        console.print(f"run folder: [italic]{(config.path).absolute()}[/italic]")

        bibout, bibtex = dokan.make_bib(runcard.data["process_name"], config.path)
        console.print(f"process: \"[bold]{runcard.data['process_name']}[/bold]\"")
        console.print(f"bibliography: [italic]{bibout.relative_to(config.path)}[/italic]")
        # console.print(f" - {bibtex.relative_to(config.path)}")
        # with open(bibout, "r") as bib:
        #     syntx = Syntax(bib.read(), "bibtex")
        #     console.print(syntx)
        with open(bibtex, "r") as bib:
            syntx = Syntax(bib.read(), "tex")
            console.print(syntx)
        # @todo please confirm that you will cite these references in you work

        config["exe"]["path"] = nnlojet_exe
        config["run"]["name"] = runcard.data["run_name"]
        config["run"]["histograms"] = runcard.data["histograms"]
        if "histograms_single_file" in runcard.data:
            config["run"]["histograms_single_file"] = runcard.data["histograms_single_file"]
        config["run"]["template"] = "template.run"
        config["process"]["name"] = runcard.data["process_name"]
        config["process"]["channels"] = dokan.nnlojet.get_lumi(
            config["exe"]["path"], config["process"]["name"]
        )
        config.write()
        runcard.to_tempalte(Path(config["run"]["path"]) / config["run"]["template"])

        # > optional default settings
        set_defaults: bool = Confirm.ask(
            f"do you want to set new defaults for the run configuration?\n(can be overwritten later as well as though CLI flags)",
            default=True,
        )
        if set_defaults:
            default_policy: ExecutionPolicy = ExecutionPolicyPrompt.ask(
                "policy",
                choices=list(str(p) for p in ExecutionPolicy),
                default=ExecutionPolicy.LOCAL,
            )
            config["exe"]["policy"] = default_policy
            console.print(f"[dim]policy = {config["exe"]["policy"]!r}[/dim]")

            default_order: Order = OrderPrompt.ask(
                "order", choices=list(str(o) for o in Order), default=Order.NNLO
            )
            config["run"]["order"] = default_order
            console.print(f"[dim]order = {config["run"]["order"]!r}[/dim]")

            while True:
                default_target_rel_acc: float = FloatPrompt.ask(
                    "target relative accuracy", default=1e-3
                )
                if default_target_rel_acc > 0.0:
                    break
                console.print("please enter a positive value")
            config["run"]["target_rel_acc"] = default_target_rel_acc
            console.print(f"[dim]target_rel_acc = {config["run"]["target_rel_acc"]!r}[/dim]")

            while True:
                default_job_max_runtime: float = FloatPrompt.ask(
                    "maximum runtime (in seconds) for individual jobs"
                )
                if default_job_max_runtime > 0.0:
                    break
                console.print("please enter a positive value")
            config["run"]["job_max_runtime"] = default_job_max_runtime
            console.print(f"[dim]job_max_runtime = {config["run"]["job_max_runtime"]!r}[/dim]")

            default_job_fill_max_runtime: bool = Confirm.ask(
                "attempt to exhaust the maximum runtime for each job?", default=True
            )
            console.print(f"[dim]job_fill_max_runtime = {default_job_fill_max_runtime!r}[/dim]")
            while True:
                default_jobs_max_total: int = IntPrompt.ask("maximum number of jobs")
                if default_jobs_max_total >= 0:
                    break
                console.print("please enter a non-negative value")
            config["run"]["jobs_max_total"] = default_jobs_max_total
            console.print(f"[dim]jobs_max_total = {config["run"]["jobs_max_total"]!r}[/dim]")

            while True:
                default_jobs_max_concurrent: int = IntPrompt.ask(
                    "maximum number of concurrently running jobs"
                )
                if default_jobs_max_concurrent > 0:
                    break
                console.print("please enter a positive value")
            config["run"]["jobs_max_concurrent"] = default_jobs_max_concurrent
            console.print(
                f"[dim]jobs_max_concurrent = {config["run"]["jobs_max_concurrent"]!r}[/dim]"
            )

            while True:
                default_jobs_batch_size: int = IntPrompt.ask("job batch size", default=1)
                if default_jobs_batch_size > 0:
                    break
                console.print("please enter a positive value")
            config["run"]["jobs_batch_size"] = default_jobs_batch_size
            console.print(f"[dim]jobs_batch_size = {config["run"]["jobs_batch_size"]!r}[/dim]")

            while True:
                default_seed_offset: int = IntPrompt.ask("seed offset", default=0)
                if default_seed_offset >= 0:
                    break
                console.print("please enter a non-negative value")
            config["run"]["seed_offset"] = default_seed_offset
            console.print(f"[dim]seed_offset = {config["run"]["seed_offset"]!r}[/dim]")

            config.write()

    # >-----
    if args.action == "submit":
        config: dokan.Config = dokan.Config(path=args.run_path, default_ok=False)

        if nnlojet_exe is not None:
            config["exe"]["path"] = nnlojet_exe

        # @todo: parse CLI args for local config overrides
        # sys.exit("we're debugging here...")
        # @todo determine # cores on this machine
        local_ncores: int = 1

        # > create the DB skeleton & activate parts
        channels: dict = config["process"].pop("channels")
        db_init = dokan.DBInit(
            config=config,
            channels=channels,
            run_tag=time.time(),
            order=2,
        )
        luigi_result = luigi.build(
            [db_init],
            worker_scheduler_factory=dokan.WorkerSchedulerFactory(),
            detailed_summary=True,
            workers=1,
            local_scheduler=True,
            log_level="WARNING",
        )  # 'WARNING', 'INFO', 'DEBUG''
        if not luigi_result:
            sys.exit("DBInit failed")

        # > actually submit the root task to run NNLOJET and spawn the monitor
        luigi_result = luigi.build(
            [
                db_init.clone(dokan.Entry),
                db_init.clone(dokan.Monitor),
            ],
            worker_scheduler_factory=dokan.WorkerSchedulerFactory(
                resources={"local_ncores": 8, "DBTask": 10, "DBDispatch": 1},
                cache_task_completion=False,
                check_complete_on_run=False,
                check_unfulfilled_deps=True,
                wait_interval=0.1,
            ),
            detailed_summary=True,
            workers=12,  # @todo set to number of cores of the machine
            local_scheduler=True,
            log_level="WARNING",
        )  # 'WARNING', 'INFO', 'DEBUG''
        if not luigi_result.scheduling_succeeded:
            console.print(luigi_result.summary_text)

        console.print(luigi_result.one_line_summary)
        # console.print(luigi_result.status)
        # console.print(luigi_result.summary_text)


if __name__ == "__main__":
    main()
