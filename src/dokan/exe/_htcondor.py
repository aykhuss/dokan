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


class HTCondorExec(Executor):
    _file_sub: str = "job.sub"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.htcondor_template: Path = Path(__file__).parent.resolve() / "lxplus.template"
        self.file_sub: Path = self.exe_data.path / self._file_sub

    def exe(self):
        HTCsettings: dict = {
            "exe": self.exe_data["exe"],
            "job_path": str(self.exe_data.path.absolute()),
            "ncores": self.exe_data["policy_settings"]["htcondor_ncores"]
            if "htcondor_ncores" in self.exe_data["policy_settings"]
            else 1,
            "start_seed": min(job["seed"] for job in self.exe_data["jobs"].values()),
            "nseed": len(self.exe_data["jobs"]),
            "input_files": ", ".join(self.exe_data["input_files"]),
        }
        with open(self.htcondor_template, "r") as t, open(self.file_sub, "w") as f:
            f.write(string.Template(t.read()).substitute(HTCsettings))

        job_env = os.environ.copy()

        HTCsubmit = subprocess.run(
            ["condor_submit", HTCondorExec._file_sub],
            env=job_env,
            cwd=self.exe_data.path,
            capture_output=True,
            text=True,)
        print(HTCsubmit)
        # > raise error if submission command failed
        HTCsubmit.check_returncode()
        # > extract the job id (if submission was successful)
        cluster_id: int = -1
        if match_id := re.match(
            r".*job\(s\) submitted to cluster\s+(\d+).*", HTCsubmit.stdout, re.DOTALL
        ):
            cluster_id = int(match_id.group(1))
            self.exe_data["policy_settings"]["htcondor_id"] = cluster_id
            self.exe_data.write()
        else:
            logger.warn(
                f"HTCondorExec failed to submit job {self.exe_data['path']} with output {HTCsubmit.stdout} and error {HTCsubmit.stderr}"
            )
            return  # this will flag the task as failed down the line
        print(f"[{cluster_id}]: {HTCsubmit.stdout}")

        # > now we need to track the job
        self._track_job()

    def _track_job(self):
        job_id: int = self.exe_data["policy_settings"]["htcondor_id"]
        poll_time: float = self.exe_data["policy_settings"]["htcondor_poll_time"]

        # match_job_id = re.compile(r"^{:d}".format(job_id))
        # for i in range(10):
        while True:
            time.sleep(poll_time)

            # HTCquery = subprocess.run(["condor_q", "-nobatch", str(self.job_id)], capture_output = True, text = True)
            # print(HTCquery)
            # for line in HTCquery.stdout.splitlines():
            #   if re.match(match_job_id, line):
            #     print(line)

            HTCquery = subprocess.run(
                ["condor_q", "-json", str(job_id)], capture_output=True, text=True
            )
            # print(HTCquery)
            if HTCquery.stdout == "":
                break

            HTCquery_json = json.loads(HTCquery.stdout)
            # > "JobStatus" codes
            # >  0 Unexpanded  U
            # >  1 Idle  I
            # >  2 Running R
            # >  3 Removed X
            # >  4 Completed C
            # >  5 Held  H
            # >  6 Submission_err  E
            count_status = [0] * 7
            for entry in HTCquery_json:
                istatus = entry["JobStatus"]
                count_status[istatus] += 1
            njobs = sum(count_status)
            print(
                "job status: R:{:d}  I:{:d}  [total:{:d}]".format(
                    count_status[2], count_status[1], njobs
                )
            )

            if njobs == 0:
                raise Exception("shouldn't get here")
                break
