import datetime
import os
import re
import string
import subprocess
import time
from pathlib import Path

from ..._types import GenericPath
from ...db._loglevel import LogLevel
from .._executor import Executor


class SlurmExec(Executor):
    _file_sub: str = "job.sub"

    @property
    def resources(self):
        return {"jobs_concurrent": self.njobs}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.slurm_template: Path = Path(
            self.exe_data["policy_settings"]["slurm_template"]
            if "slurm_template" in self.exe_data["policy_settings"]
            else self.templates()[0]  # default to first template
        )
        self.file_sub: Path = self.exe_data.path / self._file_sub
        self.njobs: int = len(self.exe_data["jobs"])

    @staticmethod
    def templates() -> list[GenericPath]:
        return [Path(__file__).parent.resolve() / "slurm.template"]

    def exe(self):
        # > recovery mode
        if (
            "slurm_id" in self.exe_data["policy_settings"]
            and self.exe_data["policy_settings"]["slurm_id"] > 0
        ):
            self._track_job()
            return

        # > populate the submission template file
        slurm_settings: dict = {
            "exe": self.exe_data["exe"],
            "job_path": str(self.exe_data.path.absolute()),
            "ncores": self.exe_data["policy_settings"]["slurm_ncores"]
            if "slurm_ncores" in self.exe_data["policy_settings"]
            else 1,
            "start_seed": min(job["seed"] for job in self.exe_data["jobs"].values()),
            "nseed": len(self.exe_data["jobs"]),
            "input_files": ", ".join(self.exe_data["input_files"]),
            "max_runtime": str(
                datetime.timedelta(seconds=int(self.exe_data["policy_settings"]["max_runtime"]))
            ),
            # "max_runtime": int(self.exe_data["policy_settings"]["max_runtime"]),
        }
        with open(self.slurm_template, "r") as t, open(self.file_sub, "w") as f:
            f.write(string.Template(t.read()).substitute(slurm_settings))

        job_env = os.environ.copy()
        job_env["OMP_NUM_THREADS"] = "{}".format(slurm_settings["ncores"])
        job_env["OMP_STACKSIZE"] = "1024M"

        cluster_id: int = -1  # init failed state
        re_cluster_id = re.compile(r"Submitted batch job\s+(\d+).*", re.DOTALL)

        for _ in range(self.exe_data["policy_settings"]["slurm_nretry"]):
            slurm_submit = subprocess.run(
                ["sbatch", SlurmExec._file_sub],
                env=job_env,
                cwd=self.exe_data.path,
                capture_output=True,
                text=True,
            )
            if slurm_submit.returncode == 0 and (
                match_id := re.match(re_cluster_id, slurm_submit.stdout)
            ):
                cluster_id = int(match_id.group(1))
                self.exe_data["policy_settings"]["slurm_id"] = cluster_id
                self.exe_data.write()
                break
            else:
                self._logger(
                    f"SlurmExec failed to submit job {self.exe_data.path}:\n"
                    + f"{slurm_submit.stdout}\n"
                    + f"{slurm_submit.stderr}",
                    LogLevel.INFO,
                )
                time.sleep(self.exe_data["policy_settings"]["slurm_retry_delay"])

        if cluster_id < 0:
            self._logger(f"SlurmExec failed to submit job {self.exe_data.path}", LogLevel.WARN)
            return  # failed job

        # > now we need to track the job
        self._track_job()

    def _track_job(self):
        job_id: int = self.exe_data["policy_settings"]["slurm_id"]
        poll_time: float = self.exe_data["policy_settings"]["slurm_poll_time"]
        nretry: int = self.exe_data["policy_settings"]["slurm_nretry"]
        retry_delay: float = self.exe_data["policy_settings"]["slurm_retry_delay"]

        while True:
            time.sleep(poll_time)

            for _ in range(nretry):
                squeue = subprocess.run(
                    ["squeue", "-h", "--job", str(job_id)], capture_output=True, text=True
                )
                if squeue.returncode == 0:
                    if squeue.stdout == "":
                        return  # job terminated: no longer in queue
                    break
                else:
                    if re.search("Invalid job id specified", squeue.stderr):
                        return  # job terminated and record no longer in scheduler
                    self._logger(
                        f"SlurmExec failed to query job [dim](job_id={job_id})[/dim]:\n"
                        + f"{squeue.stdout}\n"
                        + f"{squeue.stderr}",
                        LogLevel.INFO,
                    )
                    time.sleep(retry_delay)
