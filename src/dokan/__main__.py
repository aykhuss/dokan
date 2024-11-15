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

from .__about__ import __version__

import dokan
import dokan.nnlojet
from dokan.exe import ExecutionPolicy
from dokan.order import Order
from dokan.util import parse_time_interval
from .db._sqla import Part


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
    validate_error_message = "[prompt.invalid]Please enter a valid policy"

    def process_response(self, value: str) -> ExecutionPolicy:
        return self.response_type.argparse(value.strip())


def main() -> None:
    # > some action-global variables
    config: dokan.Config = dokan.Config(default_ok=True)
    console = Console()
    cpu_count: int = multiprocessing.cpu_count()

    parser = argparse.ArgumentParser(description="dokan: an automated NNLOJET workflow")
    parser.add_argument("--exe", dest="exe", help="path to NNLOJET executable")
    parser.add_argument("-v", "--version", action="version", version="%(prog)s " + __version__)
    subparsers = parser.add_subparsers(dest="action")

    # > subcommand: init
    parser_init = subparsers.add_parser("init", help="initialise a run")
    parser_init.add_argument("runcard", metavar="RUNCARD", help="NNLOJET runcard")
    parser_init.add_argument(
        "-o", "--output", dest="run_path", help="destination of the run directory"
    )

    # > subcommand: config
    parser_config = subparsers.add_parser("config", help="set defaults for the run configuration")
    parser_config.add_argument("run_path", metavar="RUN", help="run directory")

    # > subcommand: submit
    parser_submit = subparsers.add_parser("submit", help="submit a run")
    parser_submit.add_argument("run_path", metavar="RUN", help="run directory")
    parser_submit.add_argument(
        "--policy",
        type=ExecutionPolicy.argparse,
        choices=list(ExecutionPolicy),
        dest="policy",
        help="execution policy",
    )
    parser_submit.add_argument(
        "--order",
        type=Order.argparse,
        choices=list(Order),
        dest="order",
        help="order of the calculation",
    )
    parser_submit.add_argument("--target-rel-acc", type=float, help="target relative accuracy")
    parser_submit.add_argument(
        "--job-max-runtime", type=parse_time_interval, help="maximum runtime for a single job"
    )
    parser_submit.add_argument("--jobs-max-total", type=int, help="maximum number of jobs")
    parser_submit.add_argument(
        "--jobs-max-concurrent", type=int, help="maximum number of concurrently running jobs"
    )
    parser_submit.add_argument("--jobs-batch-size", type=int, help="job batch size")
    parser_submit.add_argument("--seed-offset", type=int, help="seed offset")

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
            syntx = Syntax(bib.read(), "tex", word_wrap=True)
            console.print(syntx)
        console.print(
            "When using results obtained with this software, you are required to cite the relevant references."
        )
        if not Confirm.ask("Please confirm that you agree to these terms"):
            sys.exit("failed to agree with the terms of use")

        config["exe"]["path"] = nnlojet_exe
        config["run"]["dokan_version"] = __version__
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
    if args.action == "init" or args.action == "config":
        if args.action == "config":  # load!
            config = dokan.Config(path=args.run_path, default_ok=False)

        console.print(
            f"setting default values for the run configuration at [italic]{str(config.path.absolute())}[/italic]"
        )
        console.print(
            'these defaults can be reconfigured later with the [italic]"config"[/italic] subcommand'
        )
        console.print(
            "consult the subcommand help `submit --help` how these settings can be overridden for each submission"
        )

        new_policy: ExecutionPolicy = ExecutionPolicyPrompt.ask(
            "policy",
            choices=list(str(p) for p in ExecutionPolicy),
            default=config["exe"]["policy"],
        )
        config["exe"]["policy"] = new_policy
        console.print(f"[dim]policy = {config['exe']['policy']!r}[/dim]")

        new_order: Order = OrderPrompt.ask(
            "order", choices=list(str(o) for o in Order), default=config["run"]["order"]
        )
        config["run"]["order"] = new_order
        console.print(f"[dim]order = {config['run']['order']!r}[/dim]")

        while True:
            new_target_rel_acc: float = FloatPrompt.ask(
                "target relative accuracy", default=config["run"]["target_rel_acc"]
            )
            if new_target_rel_acc > 0.0:
                break
            console.print("please enter a positive value")
        config["run"]["target_rel_acc"] = new_target_rel_acc
        console.print(f"[dim]target_rel_acc = {config['run']['target_rel_acc']!r}[/dim]")

        while True:
            new_job_max_runtime: float = TimeIntervalPrompt.ask(
                'maximum runtime for individual jobs with optional units {s[default],m,h,d,w} e.g. "1h 30m"',
                default=config["run"]["job_max_runtime"],
            )
            if new_job_max_runtime > 0.0:
                break
            console.print("please enter a positive value")
        config["run"]["job_max_runtime"] = new_job_max_runtime
        console.print(f"[dim]job_max_runtime = {config['run']['job_max_runtime']!r}s[/dim]")

        new_job_fill_max_runtime: bool = Confirm.ask(
            "attempt to exhaust the maximum runtime for each job?",
            default=config["run"]["job_fill_max_runtime"],
        )
        config["run"]["job_fill_max_runtime"] = new_job_fill_max_runtime
        console.print(
            f"[dim]job_fill_max_runtime = {config['run']['job_fill_max_runtime']!r}[/dim]"
        )

        while True:
            new_jobs_max_total: int = IntPrompt.ask(
                "maximum number of jobs", default=config["run"]["jobs_max_total"]
            )
            if new_jobs_max_total >= 0:
                break
            console.print("please enter a non-negative value")
        config["run"]["jobs_max_total"] = new_jobs_max_total
        console.print(f"[dim]jobs_max_total = {config['run']['jobs_max_total']!r}[/dim]")

        if config["exe"]["policy"] == ExecutionPolicy.LOCAL:
            max_concurrent_msg: str = f"maximum number of concurrent jobs [CPU count: {cpu_count}]"
            max_concurrent_def: int = min(cpu_count, config["run"]["jobs_max_concurrent"])
        else:
            max_concurrent_msg: str = "maximum number of concurrent jobs"
            max_concurrent_def: int = config["run"]["jobs_max_concurrent"]
        while True:
            new_jobs_max_concurrent: int = IntPrompt.ask(
                max_concurrent_msg, default=max_concurrent_def
            )
            if new_jobs_max_concurrent > 0:
                break
            console.print("please enter a positive value")
        config["run"]["jobs_max_concurrent"] = new_jobs_max_concurrent
        console.print(f"[dim]jobs_max_concurrent = {config['run']['jobs_max_concurrent']!r}[/dim]")

        while True:
            new_jobs_batch_size: int = IntPrompt.ask(
                "job batch size", default=config["run"]["jobs_batch_size"]
            )
            if new_jobs_batch_size > 0:
                break
            console.print("please enter a positive value")
        config["run"]["jobs_batch_size"] = new_jobs_batch_size
        console.print(f"[dim]jobs_batch_size = {config['run']['jobs_batch_size']!r}[/dim]")

        while True:
            new_seed_offset: int = IntPrompt.ask(
                "seed offset", default=config["run"]["seed_offset"]
            )
            if new_seed_offset >= 0:
                break
            console.print("please enter a non-negative value")
        config["run"]["seed_offset"] = new_seed_offset
        console.print(f"[dim]seed_offset = {config['run']['seed_offset']!r}[/dim]")

        # @todo policy settings

        # > common cluster settings
        if config["exe"]["policy"] in [ExecutionPolicy.HTCONDOR, ExecutionPolicy.SLURM]:
            cluster: str = str(config["exe"]["policy"]).lower()
            max_runtime: float = config["run"]["job_max_runtime"]
            # > polling time intervals (aim for polling every 10% of job run but at least 10s)
            default_poll_time: float = max(10.0, max_runtime / 10.0)
            if f"{cluster}_poll_time" in config["exe"]["policy_settings"]:
                default_poll_time = config["exe"]["policy_settings"][f"{cluster}_poll_time"]
            while True:
                new_poll_time: float = TimeIntervalPrompt.ask(
                    f"time interval between pinging {cluster} scheduler for job updates",
                    default=default_poll_time,
                )
                if new_poll_time > 10.0 and new_poll_time < max_runtime / 2:
                    break
                console.print(
                    f"please enter a positive value between [10, {max_runtime/2}] seconds"
                )
            config["exe"]["policy_settings"][f"{cluster}_poll_time"] = new_poll_time
            console.print(
                f"[dim]poll_time = {config['exe']['policy_settings'][f'{cluster}_poll_time']!r}s[/dim]"
            )
            # > more cluster defaults, expert user can edit config.json
            config["exe"]["policy_settings"][f"{cluster}_nretry"] = 10
            config["exe"]["policy_settings"][f"{cluster}_retry_delay"] = 30.0

        config.write()

    # >-----
    if args.action == "submit":
        config: dokan.Config = dokan.Config(path=args.run_path, default_ok=False)

        # > CLI overrides
        if nnlojet_exe is not None:
            config["exe"]["path"] = nnlojet_exe
        if args.policy is not None:
            config["exe"]["policy"] = args.policy
        if args.order is not None:
            config["run"]["order"] = args.order
        if args.target_rel_acc is not None:
            config["run"]["target_rel_acc"] = args.target_rel_acc
        if args.job_max_runtime is not None:
            config["run"]["job_max_runtime"] = args.job_max_runtime
        if args.jobs_max_total is not None:
            config["run"]["jobs_max_total"] = args.jobs_max_total
        if args.jobs_max_concurrent is not None:
            config["run"]["jobs_max_concurrent"] = args.jobs_max_concurrent
        if args.jobs_batch_size is not None:
            config["run"]["jobs_batch_size"] = args.jobs_batch_size
        if args.seed_offset is not None:
            config["run"]["seed_offset"] = args.seed_offset

        # > create the DB skeleton & activate parts
        channels: dict = config["process"].pop("channels")
        db_init = dokan.DBInit(
            config=config,
            channels=channels,
            run_tag=time.time(),
            order=config["run"]["order"],
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
        nactive: int = 0
        with db_init.session as session:
            nactive = session.query(Part).filter(Part.active.is_(True)).count()
        console.print(f"active parts: {nactive}")

        # > actually submit the root task to run NNLOJET and spawn the monitor
        console.print(f"CPU cores: {cpu_count}")
        if config["exe"]["policy"] == ExecutionPolicy.LOCAL:
            local_ncores: int = config["run"]["jobs_max_concurrent"]
        else:
            local_ncores: int = cpu_count
        luigi_result = luigi.build(
            [
                db_init.clone(dokan.Entry),
                db_init.clone(dokan.Monitor),
            ],
            worker_scheduler_factory=dokan.WorkerSchedulerFactory(
                # @todo properly set resources according to config
                resources={
                    "local_ncores": local_ncores,
                    "jobs_concurrent": config["run"]["jobs_max_concurrent"],
                    "DBTask": cpu_count + 1,
                    "DBDispatch": 1,
                },
                cache_task_completion=False,
                check_complete_on_run=False,
                check_unfulfilled_deps=True,
                wait_interval=0.1,
            ),
            detailed_summary=True,
            workers=min(cpu_count, nactive) + 1,
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
