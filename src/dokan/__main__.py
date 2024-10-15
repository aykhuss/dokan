"""The main execution of the NNLOJET workflow"""

import luigi
from luigi.execution_summary import LuigiRunResult
import dokan
from dokan.exe._exe_config import ExecutionMode, ExecutionPolicy
from dokan.exe._exe_data import ExeData
import dokan.runcard
import dokan.nnlojet
import argparse
import os
import shutil
import sys

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
        if nnlojet_exe is None:
            sys.exit("please specify an NNLOJET executable")
        if not os.path.exists(args.runcard):
            sys.exit('runcard "{}" does not exist'.format(args.runcard))
        runcard_data = dokan.runcard.parse_runcard(args.runcard)
        if "job_name" not in runcard_data:
            sys.exit(
                'invalid runcard "{}": could not find RUN block'.format(args.runcard)
            )
        # > save all to the run config file
        if args.job_path:
            config: dokan.Config = dokan.Config(args.job_path)
        else:
            config: dokan.Config = dokan.Config(
                os.path.relpath(runcard_data["job_name"])
            )
        config["exe"]["path"] = nnlojet_exe
        config["run"]["name"] = runcard_data["job_name"]
        config["process"]["name"] = runcard_data["process_name"]
        config["process"]["channels"] = dokan.nnlojet.get_lumi(
            config["exe"]["path"], config["process"]["name"]
        )
        config.write()
        # dokan.runcard.make_template(args.runcard, config["run"]["template"])
        dokan.runcard.make_template(
            args.runcard, Path(config["run"]["path"]) / "template.run"
        )

    # >-----
    if args.action == "submit":
        config: dokan.Config = dokan.Config(args.job_path, default_ok=False)
        if nnlojet_exe is not None:
            config["exe"]["path"] = nnlojet_exe
        config["run"]["batch_size"] = 123
        # sys.exit("we're debugging here...")

        # exe_args = {
        #     "exe_type": dokan.ExecutionMode.WARMUP,
        #     "channel": "LO_1",
        #     "ncall": 100,
        #     "niter": 5,
        #     "iseed": 1
        # }
        # dokan.Executor.factory(config=dokan.CONFIG, local_path=["data"], **exe_args)

        luigi_result = luigi.build(
            [
                # dokan.Production(
                #     config=dokan.CONFIG,
                #     local_path=["data", "LO_1"],
                #     channel="LO_1",
                #     iseed=123,
                # )
                dokan.Entry(
                    config=config,
                    local_path=[],
                    order=0,
                )
            ],
            worker_scheduler_factory=dokan.WorkerSchedulerFactory(
                resources={"local_ncores": 8, "DBTask": 1}, check_complete_on_run=False
            ),
            detailed_summary=True,
            workers=10,
            local_scheduler=True,
            log_level="WARNING",
        )  # 'INFO', 'DEBUG''

        print(luigi_result.one_line_summary)
        print(luigi_result.status)
        print(luigi_result.summary_text)


if __name__ == "__main__":
    main()
