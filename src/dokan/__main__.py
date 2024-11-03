"""The main execution of the NNLOJET workflow"""

import luigi
# from luigi.execution_summary import LuigiRunResult

import dokan
import dokan.nnlojet
import argparse
import os
import shutil
import sys
import time
import multiprocessing

from rich.console import Console
from rich.prompt import Prompt
from rich.syntax import Syntax

from pathlib import Path


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
        type=dokan.ExecutionPolicy.argparse,
        choices=list(dokan.ExecutionPolicy),
        default=dokan.ExecutionPolicy.LOCAL,
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
                sys.exit(f"invalid executable {path_exe}")

        # > save all to the run config file
        if args.run_path:
            config.set_path(args.run_path)
        else:
            config.set_path(os.path.relpath(runcard.data["run_name"]))
        console.print(f"created run folder at \n  [italic]{(config.path).absolute()}[/italic]")

        bibout, bibtex = dokan.make_bib(runcard.data["process_name"], config.path)
        console.print(f"identified process \"[bold]{runcard.data['process_name']}[/bold]\"")
        console.print(f"created bibliography file: [italic]{bibout.relative_to(config.path)}[/italic]")
        console.print(f" - {bibtex.relative_to(config.path)}")
        # with open(bibout, "r") as bib:
        #     syntx = Syntax(bib.read(), "bibtex")
        #     console.print(syntx)
        with open(bibtex, "r") as bib:
            syntx = Syntax(bib.read(), "tex")
            console.print(syntx)
        #@todo please confirm that you will cite these references in you work

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
