# SPDX-FileCopyrightText: © 2024-present NNLOJET
#
# SPDX-License-Identifier: MIT

"""The main execution of the NNLOJET workflow"""

import luigi
from luigi.execution_summary import LuigiRunResult
import dokan
import dokan.runcard
import dokan.nnlojet
import argparse
import os
import shutil
import sys

from rich import console


def main() -> None:
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
        dokan.CONFIG.set_exe(path=nnlojet_exe)
        if args.job_path is not None:
            dokan.CONFIG.set_job_path(args.job_path)
        else:
            dokan.CONFIG.set_job_path(os.path.relpath(runcard_data["job_name"]))
        dokan.CONFIG.set_job(name=runcard_data["job_name"])
        dokan.CONFIG.set_process(name=runcard_data["process_name"])
        dokan.CONFIG.set_process(
            channels=dokan.nnlojet.get_lumi(
                dokan.CONFIG.exe["path"], dokan.CONFIG.process["name"]
            )
        )
        dokan.CONFIG.write_config()
        dokan.runcard.make_template(args.runcard, dokan.CONFIG.job["template"])

    # >-----
    if args.action == "submit":
        dokan.CONFIG.set_job_path(args.job_path)
        dokan.CONFIG.load_config(default_ok=False)
        if nnlojet_exe is not None:
            dokan.CONFIG.set_exe(path=nnlojet_exe)
        dokan.CONFIG.set_job(batch_size=123)
        print(dokan.CONFIG.job)
        print(dokan.CONFIG.job_path)
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
                dokan.DBInit(
                    config=dokan.CONFIG,
                    local_path=[],
                    channels=dokan.CONFIG.process["channels"],
                )
            ],
            worker_scheduler_factory=dokan.WorkerSchedulerFactory(
                resources={"ncores": 8}
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
