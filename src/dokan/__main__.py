"""The main execution of the NNLOJET workflow"""

# from luigi.execution_summary import LuigiRunResult
import argparse
import multiprocessing
import os
import resource
import shutil
import signal
import sys
import time
from pathlib import Path

import random

import luigi
from rich.console import Console
from rich.prompt import Confirm, FloatPrompt, IntPrompt, Prompt, PromptBase
from rich.syntax import Syntax
from sqlalchemy import select

from .__about__ import __version__
from .bib import make_bib
from .config import Config

# from .db._dbresurrect import DBResurrect
from .db._dbmerge import MergeFinal
from .db._dbtask import DBInit
from .db._jobstatus import JobStatus
from .db._loglevel import LogLevel
from .db._sqla import Job, Log, Part
from .entry import Entry
from .exe import ExecutionPolicy, Executor
from .monitor import Monitor
from .nnlojet import get_lumi
from .order import Order
from .runcard import Runcard
from .scheduler import WorkerSchedulerFactory
from .util import parse_time_interval, patience


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
    config = Config(default_ok=True)
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
    parser_init.add_argument("--no-lumi", action="store_true", help="skip the luminosity breakdown")

    # > subcommand: config
    parser_config = subparsers.add_parser("config", help="set defaults for the run configuration")
    parser_config.add_argument("run_path", metavar="RUN", help="run directory")
    parser_config.add_argument("--merge", action="store_true", help="set default merge parameters")
    parser_config.add_argument("--advanced", action="store_true", help="advanced settings")

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
    parser_submit.add_argument("--seed-offset", type=int, help="seed offset")

    # > subcommand: finalize
    parser_finalize = subparsers.add_parser(
        "finalize", help="merge completed jobs into a final result"
    )
    parser_finalize.add_argument("run_path", metavar="RUN", help="run directory")
    parser_finalize.add_argument("--trim-threshold", type=float, help="threshold to flag outliers")
    parser_finalize.add_argument(
        "--trim-max-fraction", type=float, help="maximum fraction allowed to trim away"
    )
    parser_finalize.add_argument("--k-scan-nsteps", type=int, help="number of steps in the k-scan")
    parser_finalize.add_argument(
        "--k-scan-maxdev-steps", type=float, help="maximum deviation between k-scan steps"
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
        runcard = Runcard(runcard=args.runcard)
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

        try:
            bibout, bibtex = make_bib(runcard.data["process_name"], config.path)
            console.print(f'process: "[bold]{runcard.data["process_name"]}[/bold]"')
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
        except Exception as e:
            console.print(f"error encountered in writing bibliography files:\n{e}")

        config["exe"]["path"] = nnlojet_exe
        config["run"]["dokan_version"] = __version__
        config["run"]["name"] = runcard.data["run_name"]
        config["run"]["histograms"] = runcard.data["histograms"]
        if "histograms_single_file" in runcard.data:
            config["run"]["histograms_single_file"] = runcard.data["histograms_single_file"]
        config["run"]["template"] = "template.run"
        config["process"]["name"] = runcard.data["process_name"]
        config["process"]["channels"] = get_lumi(
            config["exe"]["path"], config["process"]["name"], use_default=args.no_lumi
        )
        config.write()
        runcard.to_tempalte(Path(config["run"]["path"]) / config["run"]["template"])

    # >-----
    if args.action == "init" or args.action == "config":
        if args.action == "config":  # load!
            config = Config(path=args.run_path, default_ok=False)

            # > advanced settings
            if args.advanced:
                while True:
                    new_seed_offset: int = IntPrompt.ask(
                        "seed offset", default=config["run"]["seed_offset"]
                    )
                    if new_seed_offset >= 0:
                        break
                    console.print("please enter a non-negative value")
                config["run"]["seed_offset"] = new_seed_offset
                console.print(f"[dim]seed_offset = {config['run']['seed_offset']!r}[/dim]")

                # console.print(
                #     "for further advanced settings edit the config.json manually & consult the documentation"
                # )

                # > config with flags skip the default config options
                return

            # > merge settings
            if args.merge:
                # @todo if settings are changed, trigger a full re-merge?
                # do by updating DB to re-set the merged counter to 0?
                while True:
                    new_trim_threshold: float = FloatPrompt.ask(
                        "trim threshold", default=config["merge"]["trim_threshold"]
                    )
                    if new_trim_threshold > 0.0:
                        break
                    console.print("please enter a positive value")
                config["merge"]["trim_threshold"] = new_trim_threshold
                console.print(f"[dim]trim_threshold = {config['merge']['trim_threshold']!r}[/dim]")

                while True:
                    new_trim_max_fraction: float = FloatPrompt.ask(
                        "trim max fraction", default=config["merge"]["trim_max_fraction"]
                    )
                    if new_trim_max_fraction > 0.0 and new_trim_max_fraction < 1.0:
                        break
                    console.print("please enter a value between 0 and 1")
                config["merge"]["trim_max_fraction"] = new_trim_max_fraction
                console.print(
                    f"[dim]trim_max_fraction = {config['merge']['trim_max_fraction']!r}[/dim]"
                )

                while True:
                    new_k_scan_nsteps: int = IntPrompt.ask(
                        "k-scan nsteps", default=config["merge"]["k_scan_nsteps"]
                    )
                    if new_k_scan_nsteps > 0:
                        break
                    console.print("please enter a positive value")
                config["merge"]["k_scan_nsteps"] = new_k_scan_nsteps
                console.print(f"[dim]k_scan_nsteps = {config['merge']['k_scan_nsteps']!r}[/dim]")

                while True:
                    new_k_scan_maxdev_steps: float = FloatPrompt.ask(
                        "k-scan maxdev steps", default=config["merge"]["k_scan_maxdev_steps"]
                    )
                    if new_k_scan_maxdev_steps > 0.0:
                        break
                    console.print("please enter a positive value")
                config["merge"]["k_scan_maxdev_steps"] = new_k_scan_maxdev_steps
                console.print(
                    f"[dim]k_scan_maxdev_steps = {config['merge']['k_scan_maxdev_steps']!r}[/dim]"
                )

                # > config with flags skip the default config options
                return

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
                    f"please enter a positive value between [10, {max_runtime / 2}] seconds"
                )
            config["exe"]["policy_settings"][f"{cluster}_poll_time"] = new_poll_time
            console.print(
                f"[dim]poll_time = {config['exe']['policy_settings'][f'{cluster}_poll_time']!r}s[/dim]"
            )
            # > more cluster defaults, expert user can edit config.json
            config["exe"]["policy_settings"][f"{cluster}_nretry"] = 10
            config["exe"]["policy_settings"][f"{cluster}_retry_delay"] = 30.0

        # > executor templates
        if len(templates := Executor.get_cls(policy=config["exe"]["policy"]).templates()) > 0:
            cluster: str = str(config["exe"]["policy"]).lower()
            # console.print(f"execution policy \"[bold]{cluster}[/bold]\" requires templates!")
            template: Path = Path(templates[0])
            if len(templates) > 1:
                console.print(f"please select one of the following built-in {cluster} templates:")
                for i, t in enumerate(templates):
                    console.print(f" [italic]{i}:[/italic] {Path(t).name}")
                it: int = IntPrompt.ask(
                    "template index",
                    choices=[str(i) for i in range(len(templates))],
                    default=0,
                )
                template = Path(templates[it])
            config["exe"]["policy_settings"][f"{cluster}_template"] = template.name
            dst = (
                Path(config["run"]["path"])
                / config["exe"]["policy_settings"][f"{cluster}_template"]
            )
            shutil.copyfile(template, dst)
            console.print(
                f"{cluster} template: [italic]{template.name}[/italic] copied to run folder:"
            )
            with open(dst, "r") as run_template:
                syntx = Syntax(run_template.read(), "shell", word_wrap=True)
                console.print(syntx)
            console.print("please edit this file to your needs")

        config.write()

    # >-----
    if args.action == "submit":
        config = Config(path=args.run_path, default_ok=False)

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
        if args.seed_offset is not None:
            config["run"]["seed_offset"] = args.seed_offset

        # > create the DB skeleton & activate parts
        channels: dict = config["process"].pop("channels")
        db_init = DBInit(
            config=config,
            channels=channels,
            run_tag=time.time(),
            order=config["run"]["order"],
        )
        luigi_result = luigi.build(
            [db_init],
            worker_scheduler_factory=WorkerSchedulerFactory(),
            detailed_summary=True,
            workers=1,
            local_scheduler=True,
            log_level="WARNING",
        )  # 'WARNING', 'INFO', 'DEBUG''
        if not luigi_result:
            sys.exit("DBInit failed")
        nactive_part: int = 0
        nactive_job: int = 0
        nfailed_job: int = 0
        with db_init.session as session:
            nactive_part = session.query(Part).filter(Part.active.is_(True)).count()
            nactive_job = session.query(Job).filter(Job.status.in_(JobStatus.active_list())).count()
            nfailed_job = session.query(Job).filter(Job.status.in_([JobStatus.FAILED])).count()
            # > clear log(?), indicate new submission
            last_log = session.scalars(select(Log).order_by(Log.id.desc())).first()
            if last_log:
                console.print(f"last log: {last_log!r}")
                if last_log.level in [LogLevel.SIG_COMP] or Confirm.ask("clear log?", default=True):
                    for log in session.scalars(select(Log)):
                        session.delete(log)
                    session.commit()
            db_init._logger(session, "submit", level=LogLevel.SIG_SUB)

        console.print(f"active parts: {nactive_part}")
        # console.print(f"active jobs: {nactive_job}")
        if nactive_part == 0:
            console.print("[red]calculation has no active part?![/red]")
            sys.exit(0)

        # > register signal handlers
        graceful_exit_triggered: bool = False

        def graceful_exit(sig, frame):
            console = Console()
            nonlocal graceful_exit_triggered
            if graceful_exit_triggered:
                console.print("\n[magenta]" + random.choice(patience) + "[/magenta]")
                return
            graceful_exit_triggered = True
            console.print(f"\n[magenta]received signal: {signal.Signals(sig).name}[/magenta]")
            nonlocal db_init
            with db_init.session as session:
                db_init._logger(
                    session,
                    f"received signal: {signal.Signals(sig).name}; let me attempt to exit gracefully...",
                    level=LogLevel.SIG_TERM,
                )
            time.sleep(1.0)
            sys.exit(0)

        signal.signal(signal.SIGINT, graceful_exit)
        signal.signal(signal.SIGTERM, graceful_exit)
        # @todo SIGUSR1 to trigger MergeAll?

        # @todo checks of the DB and ask for recovery mode?
        resurrect: list[tuple[float, str]] = []
        if nactive_job > 0:
            select_active_jobs = select(Job).where(Job.status.in_(JobStatus.active_list()))
            console.print(f"there appear to be {nactive_job} active jobs in the database")
            if Confirm.ask("attempt to recover/restart them?", default=True):
                with db_init.session as session:
                    for job in session.scalars(select_active_jobs):
                        if job.status in [JobStatus.QUEUED, JobStatus.DISPATCHED]:
                            # > queued/dispatched jobs did not execute yet:
                            # > just delete, no advantage in restoring
                            # > (possible seed gaps from dispatched but ok)
                            console.print(f" > remove (never started): {job!r}")
                            if job.rel_path is not None and Path(job.rel_path).exists():
                                shutil.rmtree(db_init._local(job.rel_path))
                            session.delete(job)
                        elif job.status == JobStatus.RUNNING:
                            console.print(f" > resurrect: {job!r}")
                            if all(r[1] != job.rel_path for r in resurrect if r[0] == job.run_tag):
                                if job.rel_path:
                                    resurrect.append((job.run_tag, job.rel_path))
                            # if all(
                            #     dbr.rel_path != job.rel_path
                            #     for dbr in resurrect
                            #     if dbr.run_tag == job.run_tag
                            # ):
                            #     resurrect.append(
                            #         db_init.clone(
                            #             DBResurrect, run_tag=job.run_tag, rel_path=job.rel_path
                            #         )
                            #     )
                        else:
                            raise RuntimeError(f"unexpected job status in recovery: {job.status}")
                    session.commit()
            else:
                if Confirm.ask("remove them from the database?", default=False):
                    with db_init.session as session:
                        for job in session.scalars(select_active_jobs):
                            console.print(f" > removing: {job!r}")
                            if job.rel_path is not None and Path(job.rel_path).exists():
                                shutil.rmtree(db_init._local(job.rel_path))
                            session.delete(job)
                        session.commit()

        if nfailed_job > 0:
            select_failed_jobs = select(Job).where(Job.status.in_([JobStatus.FAILED]))
            console.print(f"there appear to be {nfailed_job} failed jobs in the database")
            if Confirm.ask("remove them from the database?", default=True):
                with db_init.session as session:
                    for job in session.scalars(select_failed_jobs):
                        console.print(f" > removing: {job!r}")
                        if job.rel_path is not None and Path(job.rel_path).exists():
                            shutil.rmtree(db_init._local(job.rel_path))
                        session.delete(job)
                    session.commit()

        # @todo skip warmup?

        # > determine resources and dynamic job settings
        jobs_max: int = min(config["run"]["jobs_max_concurrent"], config["run"]["jobs_max_total"])
        console.print(f"# CPU cores: {cpu_count}")
        if config["exe"]["policy"] == ExecutionPolicy.LOCAL:
            local_ncores: int = jobs_max
        else:
            local_ncores: int = cpu_count
        nworkers: int = max(cpu_count, nactive_part) + 1
        config["run"]["jobs_batch_size"] = max(
            2 * (jobs_max // nactive_part) + 1,
            config["run"]["jobs_batch_unit_size"],
        )
        console.print(f"# workers: {nworkers}")
        console.print(f"# batch size: {config['run']['jobs_batch_size']}")

        # > increase limit on #files to accommodate potentially large #workers we spawn
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (10 * nworkers, resource.RLIM_INFINITY))
        except ValueError as err:
            console.print(f"failed to increase RLIMIT_NOFILE: {err}")

        # > actually submit the root task to run NNLOJET and spawn the monitor
        # > pass config since it changed w.r.t. db_init
        luigi_result = luigi.build(
            [
                db_init.clone(Entry, config=config, resurrect=resurrect),
                db_init.clone(Monitor, config=config),
                # *resurrects,
            ],
            worker_scheduler_factory=WorkerSchedulerFactory(
                # @todo properly set resources according to config
                resources={
                    "local_ncores": local_ncores,
                    "jobs_concurrent": jobs_max,
                    "DBTask": cpu_count + 2,
                    "DBDispatch": 1,
                },
                cache_task_completion=False,  # needed for MergePart
                check_complete_on_run=False,
                check_unfulfilled_deps=True,
                wait_interval=0.1,
            ),
            detailed_summary=True,
            workers=nworkers,
            local_scheduler=True,
            log_level="WARNING",
        )  # 'WARNING', 'INFO', 'DEBUG''
        if not luigi_result.scheduling_succeeded:
            console.print(luigi_result.summary_text)

        console.print("\n" + luigi_result.one_line_summary)
        # console.print(luigi_result.status)
        # console.print(luigi_result.summary_text)

        # @todo give an estimate in case target accuracy not reached.

    # >-----
    if args.action == "finalize":
        config = Config(path=args.run_path, default_ok=False)

        # > CLI overrides: persistent overwrite --> config
        if nnlojet_exe is not None:
            config["exe"]["path"] = nnlojet_exe
        # > merge settings
        if args.trim_threshold is not None:
            config["merge"]["trim_threshold"] = args.trim_threshold
        if args.trim_max_fraction is not None:
            config["merge"]["trim_max_fraction"] = args.trim_max_fraction
        if args.k_scan_nsteps is not None:
            config["merge"]["k_scan_nsteps"] = args.k_scan_nsteps
        if args.k_scan_maxdev_steps is not None:
            config["merge"]["k_scan_maxdev_steps"] = args.k_scan_maxdev_steps
        console.print(config["merge"])

        # > launch the finalization task
        mrg_final = MergeFinal(
            reset_tag=time.time(),
            config=config,
            run_tag=time.time(),
        )
        nactive_part: int = 0
        with mrg_final.session as session:
            nactive_part = session.query(Part).filter(Part.active.is_(True)).count()
            mrg_final._logger(session, "finalize", level=LogLevel.SIG_FINI)

        luigi_result = luigi.build(
            [mrg_final],
            worker_scheduler_factory=WorkerSchedulerFactory(
                resources={
                    # @todo allow `-j` flag for user to pick?
                    "local_ncores": cpu_count,
                    "DBTask": cpu_count + 1,
                },
                cache_task_completion=False,
                check_complete_on_run=False,
                check_unfulfilled_deps=True,
                wait_interval=0.1,
            ),
            detailed_summary=True,
            workers=min(cpu_count, nactive_part) + 1,
            local_scheduler=True,
            log_level="WARNING",
        )  # 'WARNING', 'INFO', 'DEBUG''
        if not luigi_result:
            sys.exit("Final failed")


if __name__ == "__main__":
    main()
