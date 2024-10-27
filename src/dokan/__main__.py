"""The main execution of the NNLOJET workflow"""

import luigi
from luigi.execution_summary import LuigiRunResult
import dokan
import dokan.nnlojet
import argparse
import os
import shutil
import sys
import time

from rich.console import Console

from pathlib import Path


def main() -> None:
    console = Console()

    parser = argparse.ArgumentParser(description="dokan: an automated NNLOJET workflow")
    parser.add_argument("--exe", dest="exe", help="executable")
    subparsers = parser.add_subparsers(dest="action")

    # > subcommand: init
    parser_init = subparsers.add_parser("init", help="initialise a job")
    parser_init.add_argument("runcard", metavar="RUN", help="NNLOJET runcard")
    parser_init.add_argument(
        "-o", "--output", dest="job_path", help="destination of the job directory"
    )

    # > subcommand: submit
    parser_submit = subparsers.add_parser("submit", help="submit jobs")
    parser_submit.add_argument("job_path", metavar="JOB", help="job directory")
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
        if os.path.isfile(args.exe) and os.access(args.exe, os.X_OK):
            nnlojet_exe = args.exe
        else:
            sys.exit('invalid executable "{}"'.format(args.exe))

    # >-----
    if args.action == "init":
        config: dokan.Config = dokan.Config(default_ok=True)
        runcard: dokan.Runcard = dokan.Runcard(runcard=args.runcard)
        console.print(runcard.data)
        if nnlojet_exe is None:
            sys.exit("please specify an NNLOJET executable")
        # > save all to the run config file
        if args.job_path:
            config.set_path(args.job_path)
        else:
            config.set_path(os.path.relpath(runcard.data["run_name"]))
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
        config: dokan.Config = dokan.Config(path=args.job_path, default_ok=False)

        if nnlojet_exe is not None:
            config["exe"]["path"] = nnlojet_exe

        # @todo: parse CLI args for local config overrides
        config["run"]["batch_size"] = 123
        # sys.exit("we're debugging here...")
        # @todo determine # cores on this machine
        local_ncores: int = 1

        channels: dict = config["process"].pop("channels")
        luigi_result = luigi.build(
            [
                dokan.Entry(
                    config=config,
                    local_path=[],
                    run_tag=time.time(),
                    channels=channels,
                    order=1,
                )
            ],
            worker_scheduler_factory=dokan.WorkerSchedulerFactory(
                resources={"local_ncores": 8, "DBTask": 10}, check_complete_on_run=False
            ),
            detailed_summary=True,
            workers=12,
            local_scheduler=True,
            log_level="WARNING",
        )  # 'WARNING', 'INFO', 'DEBUG''

        print(luigi_result.one_line_summary)
        print(luigi_result.status)
        print(luigi_result.summary_text)


if __name__ == "__main__":
    main()
