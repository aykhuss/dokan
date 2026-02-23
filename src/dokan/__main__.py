"""Command-line entrypoint for the dokan workflow.

This module defines:
- argparse conversion helpers (for booleans and structured prompt inputs),
- interactive prompt wrappers used by the `config` flow,
- the top-level `main()` command dispatcher.
"""

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

import luigi
from rich.console import Console
from rich.prompt import Confirm, FloatPrompt, IntPrompt, InvalidResponse, Prompt, PromptBase
from rich.syntax import Syntax
from sqlalchemy import select

from .__about__ import __version__
from .bib import make_bib
from .config import Config
from .db._dbinit import DBInit
from .db._dbmerge import MergeFinal
from .db._dbremovejob import DBRemoveJob
from .db._dbresurrect import DBResurrect
from .db._jobstatus import JobStatus
from .db._loglevel import LogLevel
from .db._sqla import Job, Log, Part
from .entry import Entry
from .exe import ExecutionPolicy, Executor
from .monitor import Monitor
from .nnlojet import check_PDF, dry_run, get_lumi
from .order import Order
from .runcard import Runcard, RuncardTemplate
from .scheduler import WorkerSchedulerFactory
from .util import parse_time_interval


def str2bool(value: object) -> bool:
    """Parse CLI boolean values robustly for argparse.

    Accepts common textual forms (`yes/no`, `on/off`, `true/false`) and
    numeric `0/1`. Raises `argparse.ArgumentTypeError` for invalid input.
    """
    if isinstance(value, bool):
        return value

    if isinstance(value, int):
        if value in (0, 1):
            return bool(value)
        raise argparse.ArgumentTypeError("Boolean value expected.")

    if not isinstance(value, str):
        raise argparse.ArgumentTypeError("Boolean value expected.")

    match value.strip().casefold():
        case "true" | "t" | "1" | "yes" | "y" | "on":
            return True
        case "false" | "f" | "0" | "no" | "n" | "off":
            return False
        case _:
            raise argparse.ArgumentTypeError("Boolean value expected.")


def reset_and_exit(sig: int, frame) -> None:
    """Restore terminal cursor and exit on SIGINT."""
    print("\x1b[?25h", end="", flush=True)
    sys.exit(f'\ncaught signal: "{signal.Signals(sig).name}", exiting')


# Keep Ctrl-C behavior consistent across CLI subcommands.
signal.signal(signal.SIGINT, reset_and_exit)


class TimeIntervalPrompt(PromptBase[float]):
    """Prompt parser that converts human-readable durations to seconds."""

    response_type = float
    validate_error_message = "[prompt.invalid]Please enter a valid time interval"

    def process_response(self, value: str) -> float:
        return parse_time_interval(value.strip())


class OrderPrompt(PromptBase[Order]):
    """Prompt parser for perturbative order values."""

    response_type = Order
    validate_error_message = "[prompt.invalid]Please enter a valid order"

    def process_response(self, value: str) -> Order:
        try:
            parsed: Order = Order.parse(value.strip())
        except KeyError:
            raise InvalidResponse(self.validate_error_message) from KeyError
        return parsed


class ExecutionPolicyPrompt(PromptBase[ExecutionPolicy]):
    """Prompt parser for execution backend policy values."""

    response_type = ExecutionPolicy
    validate_error_message = "[prompt.invalid]Please enter a valid policy"

    def process_response(self, value: str) -> ExecutionPolicy:
        try:
            parsed: ExecutionPolicy = ExecutionPolicy.parse(value.strip())
        except KeyError:
            raise InvalidResponse(self.validate_error_message) from KeyError
        return parsed


class LogLevelPrompt(PromptBase[LogLevel]):
    """Prompt parser for Dokan log-level values."""

    response_type = LogLevel
    validate_error_message = "[prompt.invalid]Please enter a valid log level"

    def process_response(self, value: str) -> LogLevel:
        try:
            parsed: LogLevel = LogLevel.parse(value.strip())
        except KeyError:
            raise InvalidResponse(self.validate_error_message) from KeyError
        return parsed


def main() -> None:
    """Run the Dokan CLI dispatcher and selected subcommand workflow."""
    # > some action-global variables
    config: Config = Config(default_ok=True)
    console: Console = Console()
    cpu_count: int = multiprocessing.cpu_count()

    parser = argparse.ArgumentParser(description="dokan: an automated NNLOJET workflow")
    parser.add_argument("--exe", dest="exe", help="path to NNLOJET executable")
    parser.add_argument("-v", "--version", action="version", version="%(prog)s " + __version__)
    subparsers = parser.add_subparsers(dest="action")

    # > subcommand: init
    parser_init = subparsers.add_parser("init", help="initialise a run")
    parser_init.add_argument("runcard", metavar="RUNCARD", help="NNLOJET runcard")
    parser_init.add_argument("-o", "--output", dest="run_path", help="destination of the run directory")
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
    parser_submit.add_argument("--local-cores", type=int, help="maximum number of local cores")
    parser_submit.add_argument(
        "--skip-warmup",
        help="skip the warmup stage",
        nargs="?",
        const=True,
        default=None,
        type=str2bool,
    )
    parser_submit.add_argument(
        "--live-monitor",
        help="switch on/off the live monitor",
        action=argparse.BooleanOptionalAction,
    )
    parser_submit.add_argument("--channels", nargs="+", default=None)

    # > subcommand: doctor
    parser_doctor = subparsers.add_parser("doctor", help="your workflow wellness specialist 🩺")
    parser_doctor.add_argument("run_path", metavar="RUN", help="run directory")
    parser_doctor.add_argument("--recover", action="store_true", help="recover started but incomplete jobs")

    # > subcommand: finalize
    parser_finalize = subparsers.add_parser("finalize", help="merge completed jobs into a final result")
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

    nnlojet_exe: str | None = None
    path_exe: Path
    if args.action == "init":
        nnlojet_exe = shutil.which("NNLOJET")
    if args.exe is not None:
        path_exe = Path(args.exe)
        if path_exe.is_file() and os.access(path_exe, os.X_OK):
            nnlojet_exe = str(path_exe.absolute())
        else:
            sys.exit(f"invalid executable {path_exe}")

    # >-----
    if args.action == "init":
        runcard = Runcard(runcard=args.runcard)
        if nnlojet_exe is None:
            prompt_exe = Prompt.ask("Could not find an NNLOJET executable. Please specify path")
            path_exe = Path(prompt_exe)
            if path_exe.is_file() and os.access(path_exe, os.X_OK):
                nnlojet_exe = str(path_exe.absolute())
            else:
                sys.exit(f"invalid executable {path_exe.absolute()!s}")

        # > save all to the run config file
        target_path: str = args.run_path if args.run_path else os.path.relpath(runcard.data["run_name"])
        if Path(target_path).exists() and not Confirm.ask(
            f"The folder {target_path} already exists, do you want to continue?"
        ):
            sys.exit("Please select a different output folder.")
        config.set_path(target_path)

        console.print(f"run folder: [italic]{(config.path).absolute()}[/italic]")

        config["exe"]["path"] = nnlojet_exe
        config["run"]["dokan_version"] = __version__
        config["run"]["name"] = runcard.data["run_name"]
        config["run"]["histograms"] = runcard.data["histograms"]
        if "histograms_single_file" in runcard.data:
            config["run"]["histograms_single_file"] = runcard.data["histograms_single_file"]
        config["run"]["template"] = "template.run"
        config["process"]["name"] = runcard.data["process_name"]
        # @ todo inject epem channels here
        config["process"]["channels"] = get_lumi(
            config["exe"]["path"], config["process"]["name"], use_default=args.no_lumi
        )
        for pdf in runcard.data["PDFs"]:
            if not check_PDF(config["exe"]["path"], pdf):
                raise RuntimeError(f'PDF set: "{pdf}" not found')
        run_template: RuncardTemplate = runcard.to_template(config.path / config["run"]["template"])
        config["run"]["md5"] = run_template.to_md5_hash()
        config.write()

        # > do a dry run to check that the runcard is valid
        tmp_path: Path = config.path / "tmp"
        if tmp_path.exists():
            shutil.rmtree(tmp_path)
        tmp_path.mkdir(parents=True)
        tmp_run: Path = tmp_path / "job.run"
        run_template.fill(
            tmp_run,
            sweep="warmup = 1[1]  production = 1[1]",
            run="",
            channels="LO",
            channels_region="",
            toplevel="",
        )
        dry_exe: dict = dry_run(config["exe"]["path"], tmp_path, tmp_run)
        if not dry_exe["success"]:
            console.print(f"error in dry run at {tmp_path}")
            if Confirm.ask("see output?"):
                with open(dry_exe["file_out"]) as of:
                    syntx = Syntax(of.read(), "text", word_wrap=True)
                    console.print(syntx)
            sys.exit("invalid input runcard?!")
        # else:
        #     shutil.rmtree(tmp_path)

        try:
            bibout, bibtex = make_bib(runcard.data["process_name"], config.path)
            console.print(f'process: "[bold]{runcard.data["process_name"]}[/bold]"')
            console.print(f"bibliography: [italic]{bibout.relative_to(config.path)}[/italic]")
            # console.print(f" - {bibtex.relative_to(config.path)}")
            # with open(bibout, "r") as bib:
            #     syntx = Syntax(bib.read(), "bibtex")
            #     console.print(syntx)
            with open(bibtex) as bib:
                syntx = Syntax(bib.read(), "tex", word_wrap=True)
                console.print(syntx)
            console.print(
                "When using results obtained with this software, please cite the relevant references."
            )
            if not Confirm.ask("Confirm"):
                sys.exit("failed to agree with the terms of use")
        except Exception as e:
            console.print(f"error encountered in writing bibliography files:\n{e}")

    # >-----
    if args.action in ["init", "config"]:
        if args.action == "config":  # load!
            config = Config(path=args.run_path, default_ok=False)

            # > advanced settings
            if args.advanced:
                while True:
                    new_seed_offset: int = IntPrompt.ask("seed offset", default=config["run"]["seed_offset"])
                    if new_seed_offset >= 0:
                        break
                    console.print("please enter a non-negative value")
                config["run"]["seed_offset"] = new_seed_offset
                console.print(f"[dim]seed_offset = {config['run']['seed_offset']!r}[/dim]")

                new_ui_monitor: bool = Confirm.ask(
                    "activate the live monitor?", default=config["ui"]["monitor"]
                )
                config["ui"]["monitor"] = new_ui_monitor
                console.print(f"[dim]ui_monitor = {config['ui']['monitor']!r}[/dim]")

                if config["ui"]["monitor"]:
                    while True:
                        new_ui_refresh_delay: float = TimeIntervalPrompt.ask(
                            "refresh rate of the monitor specified as the delay in seconds",
                            default=config["ui"]["refresh_delay"],
                        )
                        if new_ui_refresh_delay >= 0.1:
                            break
                        console.print("please enter a delay of at least 0.1s")
                    config["ui"]["refresh_delay"] = new_ui_refresh_delay
                    console.print(f"[dim]refresh_delay = {config['ui']['refresh_delay']!r}s[/dim]")

                new_log_level: LogLevel = LogLevelPrompt.ask(
                    "log_level",
                    choices=list(str(p) for p in LogLevel if p > 0),
                    default=config["ui"]["log_level"],
                )
                config["ui"]["log_level"] = new_log_level
                console.print(f"[dim]log_level = {config['ui']['log_level']!r}[/dim]")

                new_warmup_frozen: bool = Confirm.ask(
                    "freeze the warmup?", default=config["warmup"]["frozen"]
                )
                config["warmup"]["frozen"] = new_warmup_frozen
                console.print(f"[dim]frozen = {config['warmup']['frozen']!r}[/dim]")

                if ((raw_path := config["run"].get("raw_path")) is not None) or (
                    Confirm.ask("Store the raw data at a separate location?", default=False)
                ):
                    if raw_path is None:
                        raw_path = Prompt.ask("New path for raw data")
                        if not raw_path.endswith("/" + config.path.name):
                            raw_path = raw_path + "/" + config.path.name
                    else:
                        console.print(f"current raw data path: [italic]{raw_path}[/italic]")
                        if Confirm.ask("Do you want to change the raw data path?", default=False):
                            sys.exit("Changing raw path through the workflow is not supported yet.")
                    raw_path = Path(raw_path)
                    new_raw_path: Path = Path(
                        Prompt.ask("Path for raw data", default=str(raw_path.absolute()))
                    )
                    if not new_raw_path.exists():
                        new_raw_path.mkdir(parents=True, exist_ok=True)
                    config["run"]["raw_path"] = str(new_raw_path.absolute())
                    console.print(f"[dim]raw_path = {config['run']['raw_path']!r}[/dim]")

                console.print(
                    "[dim]more advanced settings in config.json (consult documentation in src/dokan/config.py)[/dim]"
                )

                # > config with flags skip the default config options
                config.write()
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
                console.print(f"[dim]trim_max_fraction = {config['merge']['trim_max_fraction']!r}[/dim]")

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
                console.print(f"[dim]k_scan_maxdev_steps = {config['merge']['k_scan_maxdev_steps']!r}[/dim]")

                # > config with flags skip the default config options
                config.write()
                return

        console.print(
            f"setting default values for the run configuration at [italic]{config.path.absolute()!s}[/italic]"
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
                'maximum runtime for individual jobs with optional units {s[default],m,h,d,w} e.g. "1h 10m"',
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
        console.print(f"[dim]job_fill_max_runtime = {config['run']['job_fill_max_runtime']!r}[/dim]")

        while True:
            new_jobs_max_total: int = IntPrompt.ask(
                "maximum number of jobs", default=config["run"]["jobs_max_total"]
            )
            if new_jobs_max_total >= 0:
                break
            console.print("please enter a non-negative value")
        config["run"]["jobs_max_total"] = new_jobs_max_total
        console.print(f"[dim]jobs_max_total = {config['run']['jobs_max_total']!r}[/dim]")

        max_concurrent_msg: str
        max_concurrent_def: int
        if config["exe"]["policy"] == ExecutionPolicy.LOCAL:
            max_concurrent_msg = f"maximum number of concurrent jobs [CPU count: {cpu_count}]"
            max_concurrent_def = min(cpu_count, config["run"]["jobs_max_concurrent"])
        else:
            max_concurrent_msg = "maximum number of concurrent jobs"
            max_concurrent_def = config["run"]["jobs_max_concurrent"]
        while True:
            new_jobs_max_concurrent: int = IntPrompt.ask(max_concurrent_msg, default=max_concurrent_def)
            if new_jobs_max_concurrent > 0:
                break
            console.print("please enter a positive value")
        config["run"]["jobs_max_concurrent"] = new_jobs_max_concurrent
        console.print(f"[dim]jobs_max_concurrent = {config['run']['jobs_max_concurrent']!r}[/dim]")

        # @todo policy settings

        # > common cluster settings
        cluster: str
        if config["exe"]["policy"] in [ExecutionPolicy.HTCONDOR, ExecutionPolicy.SLURM]:
            cluster = str(config["exe"]["policy"]).lower()
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
                console.print(f"please enter a positive value between [10, {max_runtime / 2}] seconds")
            config["exe"]["policy_settings"][f"{cluster}_poll_time"] = new_poll_time
            console.print(
                f"[dim]poll_time = {config['exe']['policy_settings'][f'{cluster}_poll_time']!r}s[/dim]"
            )
            # > more cluster defaults, expert user can edit config.json
            config["exe"]["policy_settings"][f"{cluster}_nretry"] = 10
            config["exe"]["policy_settings"][f"{cluster}_retry_delay"] = 30.0

        # > executor templates
        if len(exe_templates := Executor.get_cls(policy=config["exe"]["policy"]).templates()) > 0:
            cluster = str(config["exe"]["policy"]).lower()
            # console.print(f"execution policy \"[bold]{cluster}[/bold]\" requires templates!")
            exe_template: Path = Path(exe_templates[0])
            if len(exe_templates) > 1:
                console.print(f"please select one of the following built-in {cluster} templates:")
                for i, t in enumerate(exe_templates):
                    console.print(f" [italic]{i}:[/italic] {Path(t).name}")
                it: int = IntPrompt.ask(
                    "template index",
                    choices=[str(i) for i in range(len(exe_templates))],
                    default=0,
                )
                exe_template = Path(exe_templates[it])
            config["exe"]["policy_settings"][f"{cluster}_template"] = exe_template.name
            dst = config.path / config["exe"]["policy_settings"][f"{cluster}_template"]
            shutil.copyfile(exe_template, dst)
            console.print(f"{cluster} template: [italic]{exe_template.name}[/italic] copied to run folder:")
            with open(dst) as run_exe_template:
                syntx = Syntax(run_exe_template.read(), "shell", word_wrap=True)
                console.print(syntx)
            console.print("please edit this file to your needs")

        config.write()

    # >-----
    # > common settings & DBInit task
    channels: dict
    select_channels: list = []
    db_init: DBInit | None = None
    nactive_part: int = -1
    nactive_job: int = -1
    nfailed_job: int = -1

    if args.action in ["submit", "doctor", "finalize"]:
        config = Config(path=args.run_path, default_ok=False)
        channels = config["process"].pop("channels")

        # > CLI overrides: persistent overwrite --> config
        if nnlojet_exe is not None:
            config["exe"]["path"] = nnlojet_exe
        match args.action:
            case "submit":
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
                if args.skip_warmup is not None:
                    config["warmup"]["frozen"] = args.skip_warmup
                    console.print(f"[dim]skip_warmup = {config['warmup']['frozen']!r}[/dim]")
                if args.live_monitor is not None:
                    config["ui"]["monitor"] = args.live_monitor
                    console.print(f"[dim]live_monitor = {config['ui']['monitor']!r}[/dim]")
                if args.channels is not None:
                    for ch in args.channels:
                        if matches := [
                            key
                            for key in channels
                            if key.upper() == ch.upper() or key.upper().startswith(ch.upper() + "_")
                        ]:
                            select_channels.extend(matches)
                        else:
                            console.print(f" > channel {ch!r} did not match, skipping")
                    if not select_channels:
                        console.print(f'no channels selected with "{args.channels}", exiting')
                        sys.exit(0)
            case "doctor":
                # > no monitor needed for doctor
                config["ui"]["monitor"] = False
            case "finalize":
                # > merge settings
                if args.trim_threshold is not None:
                    config["merge"]["trim_threshold"] = args.trim_threshold
                if args.trim_max_fraction is not None:
                    config["merge"]["trim_max_fraction"] = args.trim_max_fraction
                if args.k_scan_nsteps is not None:
                    config["merge"]["k_scan_nsteps"] = args.k_scan_nsteps
                if args.k_scan_maxdev_steps is not None:
                    config["merge"]["k_scan_maxdev_steps"] = args.k_scan_maxdev_steps
                # > no monitor needed for finalize
                config["ui"]["monitor"] = False
                console.print(config["merge"])

        # > create the master Init task that also defines the `run_tag` for this execution round
        sav_monitor: bool = config["ui"]["monitor"]
        config["ui"]["monitor"] = False
        db_init = DBInit(
            config=config,  # override in "submit" action with `config`
            channels=channels,
            select_channels=select_channels,
            run_tag=time.time(),
            order=config["run"]["order"],
        )
        config["ui"]["monitor"] = sav_monitor

        # > make sure that the DB is initialised
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

        # > clear any old logs as well as jobs that were not assigned a run path
        with db_init.session as session:
            # > clear log(?), indicate new submission
            last_log = session.scalars(select(Log).order_by(Log.id.desc())).first()
            if last_log:
                console.print(f"last log: {last_log!r}")
                if last_log.level in [LogLevel.SIG_COMP, LogLevel.SIG_SUB] or Confirm.ask(
                    "clear log?", default=True
                ):
                    for log in session.scalars(select(Log)):
                        session.delete(log)
                    db_init._safe_commit(session)
            db_init._logger(session, args.action, level=LogLevel.SIG_SUB)
            # > clear pre-dispatched jobs
            console.print("purge all jobs that never started...")
            for job in session.scalars(select(Job)):
                if job.rel_path is None:
                    # > jobs that were not assigned a run path can be safely removed
                    # > should be jobs in the `QUEUED` status
                    console.print(f" > {job!r}")
                    assert job.status in [JobStatus.QUEUED, JobStatus.DISPATCHED]
                    session.delete(job)
            db_init._safe_commit(session)

        # > do a passive `DBRessurect` on active/failed jobs to update the DB according to the file system
        # > we include FAILED, because resurecction will also update FAILED jobs if data is found on disk
        # > we only need a list of run paths and DBResurrect does the rest
        recover_jobs: dict = {}
        with db_init.session as session:
            for job in session.scalars(
                select(Job).where(Job.status.in_([*JobStatus.active_list(), JobStatus.FAILED]))
            ):
                if job.rel_path is None:
                    console.print(f" > active w/o path?! {job!r}")
                    continue
                recover_jobs[job.id] = job.to_dict()  # save original state for recovery
                job.status = JobStatus.RECOVER  # mark for recovery
            db_init._safe_commit(session)
        # > launch resurrection jobs
        # > (the way we construct it, guanranteed that `recover_jobs` not empty <=> passive mode)
        luigi_result = luigi.build(
            [
                db_init.clone(
                    DBResurrect,
                    rel_path=rp,
                    recover_jobs={k: v for k, v in recover_jobs.items() if v.get("rel_path") == rp},
                )
                for rp in {jd["rel_path"] for jd in recover_jobs.values()}
            ],
            worker_scheduler_factory=WorkerSchedulerFactory(),
            detailed_summary=True,
            workers=1,
            local_scheduler=True,
            log_level="WARNING",
        )  # 'WARNING', 'INFO', 'DEBUG''
        if not luigi_result:
            sys.exit("DBResurrect failed")

        # > clean up failed jobs
        failed_jobs: dict = {}
        with db_init.session as session:
            for job in session.scalars(select(Job).where(Job.status.in_([JobStatus.FAILED]))):
                if job.rel_path is None:
                    console.print(f" > failed w/o path?! {job!r}")
                    continue
                failed_jobs[job.id] = job.to_dict()  # store full job entry
        if failed_jobs:
            console.print(f"there are {len(failed_jobs)} [bold][red]FAILED[/red][/bold] jobs in the database")
            if Confirm.ask("remove them from the database?", default=True):
                # > launch job deletion tasks
                luigi_result = luigi.build(
                    [db_init.clone(DBRemoveJob, job_id=job_id) for job_id in failed_jobs],
                    worker_scheduler_factory=WorkerSchedulerFactory(),
                    detailed_summary=True,
                    workers=1,
                    local_scheduler=True,
                    log_level="WARNING",
                )  # 'WARNING', 'INFO', 'DEBUG''
                if not luigi_result:
                    sys.exit("DBRemoveJob failed")

        # > collect job statistics
        with db_init.session as session:
            nactive_part = session.query(Part).filter(Part.active.is_(True)).count()
            nactive_job = session.query(Job).filter(Job.status.in_(JobStatus.active_list())).count()
            nfailed_job = session.query(Job).filter(Job.status.in_([JobStatus.FAILED])).count()
        console.print(f"active parts: {nactive_part}")
        if nactive_part == 0:
            console.print("[red]calculation has no active part?![/red]")
            sys.exit(0)
        console.print(f"active jobs: {nactive_job}")
        console.print(f"failed jobs: {nfailed_job}")

    # sys.exit(0)

    # >-----
    if args.action == "submit":
        if db_init is None:
            raise ValueError("DBInit is not initialized")

        # > resurrect active jobs that did not terminate properly in a past run
        resurrect_jobs: dict = {}
        with db_init.session as session:
            for job in session.scalars(select(Job).where(Job.status.in_(JobStatus.active_list()))):
                if job.rel_path is None:
                    console.print(f" > active w/o path?! {job!r}")
                    continue
                resurrect_jobs[job.id] = job.to_dict()  # store full job entry
        if resurrect_jobs:
            console.print(
                f"there are {len(resurrect_jobs)} [bold][green]ACTIVE[/green][/bold] jobs in the database"
            )
            if Confirm.ask("attempt to recover/restart them?", default=True):
                pass
            elif Confirm.ask("remove them from the database?", default=False):
                # > launch job deletion tasks
                luigi_result = luigi.build(
                    [db_init.clone(DBRemoveJob, job_id=job_id) for job_id in resurrect_jobs],
                    worker_scheduler_factory=WorkerSchedulerFactory(),
                    detailed_summary=True,
                    workers=1,
                    local_scheduler=True,
                    log_level="WARNING",
                )  # 'WARNING', 'INFO', 'DEBUG''
                if not luigi_result:
                    sys.exit("DBRemoveJob failed")
                resurrect_jobs = {}

        # @todo skip warmup?

        # > determine resources and dynamic job settings
        jobs_max: int = min(config["run"]["jobs_max_concurrent"], config["run"]["jobs_max_total"])
        console.print(f"# CPU cores: {cpu_count}")
        local_ncores: int
        local_ncores = jobs_max + 1 if config["exe"]["policy"] == ExecutionPolicy.LOCAL else cpu_count
        # > CLI override
        if args.local_cores is not None:
            local_ncores = max(2, args.local_cores)

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
                db_init.clone(Entry, config=config, resurrect_jobs=resurrect_jobs),
                db_init.clone(Monitor, config=config),
            ],
            worker_scheduler_factory=WorkerSchedulerFactory(
                # @todo properly set resources according to config
                resources={
                    "local_ncores": local_ncores,
                    "jobs_concurrent": jobs_max,
                    "DBTask": min(cpu_count, nactive_part) + 2,
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

        # console.print("\n" + luigi_result.one_line_summary)
        # console.print(luigi_result.status)
        # console.print(luigi_result.summary_text)

    # >-----
    if args.action == "doctor":
        if db_init is None:
            raise ValueError("DBInit is not initialized")

        if args.recover:
            resurrect_tasks = []
            with db_init.session as session:
                # Traverse all jobs in a status that indicates the run has started
                # DISPATCHED means it was sent to the executor, RUNNING means it started execution
                select_started_jobs = select(Job).where(
                    Job.status.in_([JobStatus.RUNNING, JobStatus.DISPATCHED])
                )
                seen_paths = set()
                for job in session.scalars(select_started_jobs):
                    # Flag jobs as under recovery
                    job.status = JobStatus.RECOVER

                    if job.rel_path and job.rel_path not in seen_paths:
                        resurrect_tasks.append(
                            db_init.clone(
                                DBResurrect,
                                run_tag=job.run_tag,
                                rel_path=job.rel_path,
                                only_recover=True,
                            )
                        )
                        seen_paths.add(job.rel_path)

                db_init._safe_commit(session)

            if resurrect_tasks:
                console.print(f"yielding {len(resurrect_tasks)} recovery tasks")
                luigi.build(
                    resurrect_tasks,
                    worker_scheduler_factory=WorkerSchedulerFactory(),
                    detailed_summary=True,
                    workers=cpu_count,
                    local_scheduler=True,
                    log_level="WARNING",
                )

            # Report final status
            with db_init.session as session:
                nactive_job = session.query(Job).filter(Job.status.in_(JobStatus.active_list())).count()
                nfailed_job = session.query(Job).filter(Job.status.in_([JobStatus.FAILED])).count()
            console.print("after recovery:")
            console.print(f"active jobs: {nactive_job}")
            console.print(f"failed jobs: {nfailed_job}")

    # >-----
    if args.action == "finalize":
        if db_init is None:
            raise ValueError("DBInit is not initialized")

        # > launch the finalization task
        mrg_final = MergeFinal(
            reset_tag=time.time(),
            config=config,
            run_tag=time.time(),
            grids=True,
        )
        with mrg_final.session as session:
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
