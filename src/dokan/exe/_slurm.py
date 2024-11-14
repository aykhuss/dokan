import re
import time
import subprocess
import logging
import string
import os
import json

from pathlib import Path

from ._executor import Executor


logger = logging.getLogger("luigi-interface")


class SlurmExec(Executor):
    _file_sub: str = "job.sub"

    @property
    def resources(self):
        return {"jobs_concurrent": self.njobs}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.slurm_template: Path = Path(__file__).parent.resolve() / "slurm.template"
        self.file_sub: Path = self.exe_data.path / self._file_sub
        self.njobs: int = len(self.exe_data["jobs"])

    def exe(self):
        slurm_settings: dict = {
            "exe": self.exe_data["exe"],
            "job_path": str(self.exe_data.path.absolute()),
            "ncores": self.exe_data["policy_settings"]["slurm_ncores"]
            if "slurm_ncores" in self.exe_data["policy_settings"]
            else 1,
            "start_seed": min(job["seed"] for job in self.exe_data["jobs"].values()),
            "nseed": len(self.exe_data["jobs"]),
            "input_files": ", ".join(self.exe_data["input_files"]),
            "max_runtime": int(self.exe_data["policy_settings"]["max_runtime"]),
        }
        with open(self.slurm_template, "r") as t, open(self.file_sub, "w") as f:
            f.write(string.Template(t.read()).substitute(slurm_settings))

        job_env = os.environ.copy()

        cluster_id: int = -1  # init failed state
        re_cluster_id = re.compile(r".*job\(s\) submitted to cluster\s+(\d+).*", re.DOTALL)

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
                logger.info(f"SlurmExec failed to submit job {self.exe_data['path']}:")
                logger.info(f"{slurm_submit.stdout}\n{slurm_submit.stderr}")
                time.sleep(self.exe_data["policy_settings"]["slurm_retry_delay"])

        if cluster_id < 0:
            logger.warn(f"SlurmExec failed to submit job {self.exe_data['path']}")
            return  # failed job

        # > now we need to track the job
        self._track_job()

    def _track_job(self):
        job_id: int = self.exe_data["policy_settings"]["slurm_id"]
        poll_time: float = self.exe_data["policy_settings"]["slurm_poll_time"]
        nretry: int = self.exe_data["policy_settings"]["slurm_nretry"]
        retry_delay: float = self.exe_data["policy_settings"]["slurm_retry_delay"]

        # match_job_id = re.compile(r"^{:d}".format(job_id))
        # for i in range(10):
        while True:
            time.sleep(poll_time)

            # squeue = subprocess.run(["squeue", "-nobatch", str(self.job_id)], capture_output = True, text = True)
            # print(squeue)
            # for line in squeue.stdout.splitlines():
            #   if re.match(match_job_id, line):
            #     print(line)
            squeue_json: dict = {}
            for _ in range(nretry):
                squeue = subprocess.run(
                    ["squeue", "-h --job", str(job_id)], capture_output=True, text=True
                )
                if squeue.returncode == 0:
                    if squeue.stdout == "":
                        return  # job terminated: no longer in queue
                    squeue_json = json.loads(squeue.stdout)
                    break
                else:
                    logger.info(f"SlurmExec failed to query job {job_id}:")
                    logger.info(f"{squeue.stdout}\n{squeue.stderr}")
                    time.sleep(retry_delay)

            # > "JobStatus" codes
            # >  0 Unexpanded  U
            # >  1 Idle  I
            # >  2 Running R
            # >  3 Removed X
            # >  4 Completed C
            # >  5 Held  H
            # >  6 Submission_err  E
            count_status = [0] * 7
            for entry in squeue_json:
                istatus = entry["JobStatus"]
                count_status[istatus] += 1
            njobs = sum(count_status)
            # print(
            #     "job[{:d}] status: R:{:d}  I:{:d}  [total:{:d}]".format(
            #         job_id, count_status[2], count_status[1], njobs
            #     )
            # )

            if njobs == 0:
                logger.warn(f"SlurmExec failed to query job {job_id} with njobs = {njobs}")
                return
