"""NNLOJET interface

helperfunctions to extract information from NNLOJET
"""

import math
import os
import re
import subprocess
from pathlib import Path

from ._types import GenericPath
from .order import Order

# @todo complete this list
_proc_has_regions: list = [
    "EPEM",
    "EEJJJ",
    "DIS",
    "EPLJJ",
    "DISWP",
    "EPNBJJ",
    "DISWM",
    "EPNJJ",
    "1JET",
    "1JETFC",
    "2JET",
    "2JETFC",
    "JJ",
    "ZJ",
    "WPJ",
    "WMJ",
    "GJ",
    "HJ",
    "HTO2PJ",
    "HTO2TAUJ",
    "HTO2L1PJ",
    "HTO4EJ",
    "HTO2E2MUJ",
    "HTO2L2NJ",
]

_default_chan_list: dict = {
    "LO": {"string": "LO", "part": "LO", "part_num": 1, "order": 0},
    "R": {"string": "R", "part": "R", "part_num": 1, "order": -1},
    "V": {"string": "V", "part": "V", "part_num": 1, "order": -1},
    "RR": {"string": "RR", "part": "RR", "part_num": 1, "order": -2, "region": "all"},
    "RV": {"string": "RV", "part": "RV", "part_num": 1, "order": -2},
    "VV": {"string": "VV", "part": "VV", "part_num": 1, "order": -2},
}
_override_chan_list: dict = {
    "eeJJJ": {
        "LO_1": {"string": "1 2", "part": "LO", "part_num": 1, "order": 0},
        "R_1": {"string": "3 4 5 6", "part": "R", "part_num": 1, "order": -1},
        "R_2": {"string": "7 8 9 10", "part": "R", "part_num": 2, "order": -1},
        "R_3": {"string": "11 12", "part": "R", "part_num": 3, "order": -1},
        "V_1": {"string": "13 14", "part": "V", "part_num": 1, "order": -1},
        "V_2": {"string": "15 16", "part": "V", "part_num": 2, "order": -1},
        "V_3": {"string": "17 18", "part": "V", "part_num": 3, "order": -1},
        "RRa_1": {
            "string": "19 20",
            "part": "RR",
            "part_num": 1,
            "region": "a",
            "order": -2,
        },
        "RRa_2": {
            "string": "21 22",
            "part": "RR",
            "part_num": 2,
            "region": "a",
            "order": -2,
        },
        "RRa_3": {
            "string": "23 24",
            "part": "RR",
            "part_num": 3,
            "region": "a",
            "order": -2,
        },
        "RRa_4": {
            "string": "25 26 27 28",
            "part": "RR",
            "part_num": 4,
            "region": "a",
            "order": -2,
        },
        "RRa_5": {
            "string": "29 30 31 32",
            "part": "RR",
            "part_num": 5,
            "region": "a",
            "order": -2,
        },
        "RRa_6": {
            "string": "33 34 35 36",
            "part": "RR",
            "part_num": 6,
            "region": "a",
            "order": -2,
        },
        "RRb_1": {
            "string": "19 20",
            "part": "RR",
            "part_num": 1,
            "region": "b",
            "order": -2,
        },
        "RRb_2": {
            "string": "21 22",
            "part": "RR",
            "part_num": 2,
            "region": "b",
            "order": -2,
        },
        "RRb_3": {
            "string": "23 24",
            "part": "RR",
            "part_num": 3,
            "region": "b",
            "order": -2,
        },
        "RRb_4": {
            "string": "25 26 27 28",
            "part": "RR",
            "part_num": 4,
            "region": "b",
            "order": -2,
        },
        "RRb_5": {
            "string": "29 30 31 32",
            "part": "RR",
            "part_num": 5,
            "region": "b",
            "order": -2,
        },
        "RRb_6": {
            "string": "33 34 35 36",
            "part": "RR",
            "part_num": 6,
            "region": "b",
            "order": -2,
        },
        "RV_1": {
            "string": "37 38 43 44",
            "part": "RV",
            "part_num": 1,
            "order": -2,
        },
        "RV_2": {
            "string": "39 40 45 46",
            "part": "RV",
            "part_num": 2,
            "order": -2,
        },
        "RV_3": {"string": "41 42", "part": "RV", "part_num": 3, "order": -2},
        "RV_4": {
            "string": "47 48 49 50 55 56 57 58",
            "part": "RV",
            "part_num": 4,
            "order": -2,
        },
        "RV_5": {
            "string": "51 52 53 54",
            "part": "RV",
            "part_num": 5,
            "order": -2,
        },
        "RV_6": {
            "string": "59 60 61 62 63 64",
            "part": "RV",
            "part_num": 6,
            "order": -2,
        },
        "VV_1": {"string": "65 66", "part": "VV", "part_num": 1, "order": -2},
        "VV_2": {"string": "67 68", "part": "VV", "part_num": 2, "order": -2},
        "VV_3": {"string": "69 70", "part": "VV", "part_num": 3, "order": -2},
        "VV_4": {"string": "71 72", "part": "VV", "part_num": 4, "order": -2},
        "VV_5": {"string": "73 74", "part": "VV", "part_num": 5, "order": -2},
        "VV_6": {"string": "75 76", "part": "VV", "part_num": 6, "order": -2},
    },
}


def dry_run(exe: GenericPath, tmp: GenericPath, runcard: GenericPath) -> dict:
    # print(f"exe = {exe!r}, tmp = {tmp!r}, runcard = {runcard!r}")

    # > extra output & error files
    file_out: Path = Path(tmp) / "job.out"
    file_err: Path = Path(tmp) / "job.err"

    job_env = os.environ.copy()
    job_env["OMP_NUM_THREADS"] = "1"
    job_env["OMP_STACKSIZE"] = "1024M"

    with open(file_out, "w") as of, open(file_err, "w") as ef:
        _ = subprocess.run(
            [
                exe,
                "-run",
                str(Path(runcard).relative_to(tmp)),
                "-iseed",
                "42",
            ],
            env=job_env,
            cwd=tmp,
            stdout=of,
            stderr=ef,
            text=True,
        )

    # > returncode = 0 does not mean success
    success: bool = False
    with open(file_out, "r") as f:
        for ln in f:
            if re.search(r"Elapsed time", ln, re.IGNORECASE):
                success = True

    return {
        "success": success,
        "file_out": str(file_out.absolute()),
        "file_err": str(file_err.absolute()),
    }


def check_PDF(exe: GenericPath, PDF: str) -> bool:
    """check if NNLOJET can find the PDF set

    use NNLOJET compiled with LHAPDF to init a PDF set and use the error
    return code to determine if the PDF set was available.

    Parameters
    ----------
    exe : GenericPath
        path to the NNLOJET executable
    PDF : str
        LHAPDF PDF set name

    Returns
    -------
    bool
        True is PDF set could be loaded successfully, False otherwise
    """
    try:
        exe_out = subprocess.run([exe, "--checkpdf", PDF], capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError:
        return False
    if exe_out.returncode != 0:
        return False
    else:
        return True


def get_lumi(exe: GenericPath, proc: str, use_default: bool = False) -> dict:
    """get channels for an NNLOJET process

    get the channels with the "part" & "lumi" information collected in groups
    that correspond to independent PDF luminosities of the process.

    Parameters
    ----------
    exe : GenericPath
        path to the NNLOJET executable
    proc : str
        NNLOJET process name
    use_default : bool, optional
        flag to force the default channel list without lumi breakdown
        (the default is False, which parses NNLOJET lumi info)

    Returns
    -------
    dict
        channel/luminosity information following the structure:
        label = "RRa_42" -> {
          "part" : "RR", ["region" : "a"]
          "part_num" : 42,
          "string" : "1 2 3 ... ! channel: ...",
          "order" : Order.NNLO_ONLY,
        }

    Raises
    ------
    RuntimeError
        encountered parsing error of the -listobs output
    """
    chan_list = dict()
    if proc in _override_chan_list:
        chan_list = _override_chan_list[proc]
    else:
        exe_out = subprocess.run([exe, "-listlumi", proc], capture_output=True, text=True, check=True)
        if exe_out.returncode != 0:
            raise RuntimeError(f"get_lumi: failed calling NNLOJET: {exe_out.stderr}")
        for line in exe_out.stdout.splitlines():
            if re.search(r"unknown process", line):
                raise RuntimeError("NNLOJET: " + line)
            if not re.search(r" ! channel: ", line):
                continue
            label = None
            chan = dict()
            match = re.match(r"^\s*(\w+)\s+(.*)$", line)
            if match:
                label = match.group(1)
                chan["string"] = match.group(2)
            else:
                raise RuntimeError("couldn't parse channel line")
            match = re.match(r"^([^_]+)_(\d+)$", label)
            if match:
                chan["part"] = match.group(1)
                chan["part_num"] = int(match.group(2))
                if chan["part"][-1] == "a" or chan["part"][-1] == "b":
                    chan["region"] = chan["part"][-1]
                    chan["part"] = chan["part"][:-1]
                chan["order"] = Order.partparse(chan["part"])
            else:
                raise RuntimeError("couldn't parse channel line")
            chan_list[label] = chan
    if use_default or not chan_list:
        if not use_default and not chan_list:
            print("could not parse luminoisty channels from NNLOJET")
            # raise RuntimeError("get_lumi: no luminosity channels parsed")
            print("defaulting to channels without luminosity breakdown")
        chan_list = _default_chan_list
        if proc.upper() in _proc_has_regions:
            chan_RR = chan_list.pop("RR")
            for region in ["a", "b"]:
                chan_RRreg = chan_RR.copy()
                chan_RRreg["region"] = "a"
                chan_list[f"RR{region}"] = dict(chan_RRreg)
        else:
            chan_list["RR"].pop("region")
        # @todo remove orders if non-existent?
        # e.g. "ZJJ" has no NNLO, only NLO: chan_list.pop("RR") &RV & VV

    return chan_list


def parse_log_file(log_file: GenericPath) -> dict:
    """parse information from an NNLOJET log file

    Parameters
    ----------
    log_file : GenericPath
        path to the log file

    Returns
    -------
    dict
        parsed information as a dictionary following the structure of:
        ExeData["jobs"][<id>]["iterations"]

    Raises
    ------
    RuntimeError
        encountered parsing error of log file
    """
    job_data: dict = {}
    job_data["iterations"] = []
    # > parse the output file to extract some information
    with open(log_file, "r") as lf:
        iteration = {}
        for line in lf:
            if match := re.search(r"\(\s*iteration\s+(\d+)\s*\)", line, re.IGNORECASE):
                iteration["iteration"] = int(match.group(1))

            if match := re.search(
                r"\bintegral\s*=\s*(\S+)\s+accum\.\s+integral\s*=\s*(\S+)\b",
                line,
                re.IGNORECASE,
            ):
                iteration["result"] = float(match.group(1))
                iteration["result_acc"] = float(match.group(2))

            if match := re.search(
                r"\bstd\.\s+dev\.\s*=\s*(\S+)\s+accum\.\s+std\.\s+dev\s*=\s*(\S+)\b",
                line,
                re.IGNORECASE,
            ):
                iteration["error"] = float(match.group(1))
                iteration["error_acc"] = float(match.group(2))

            if match := re.search(r"\schi\*\*2/iteration\s*=\s*(\S+)\b", line, re.IGNORECASE):
                iteration["chi2dof"] = float(match.group(1))
                job_data["iterations"].append(iteration)
                iteration = {}

            if match := re.search(r"\s*Elapsed\s+time\s*=\s*(\S+)\b\s*(\S+)\b", line, re.IGNORECASE):
                unit_time: str = match.group(2).lower()
                match unit_time:
                    case "seconds":
                        fac_time = 1.0
                    case "minutes":
                        fac_time = 60.0
                    case "hours":
                        fac_time = 3600.0
                    case _:
                        raise RuntimeError(f"unknown time unit: {unit_time}")

                job_data["elapsed_time"] = fac_time * float(match.group(1))
                # > the accumulated results
                if job_data["iterations"]:
                    job_data["result"] = job_data["iterations"][-1]["result_acc"]
                    job_data["error"] = job_data["iterations"][-1]["error_acc"]
                    if math.isnan(job_data["result"]):
                        # > catch the case where the integral vanishes identically
                        if all(it["result"] == 0.0 and it["error"] == 0.0 for it in job_data["iterations"]):
                            job_data["result"] = 0.0
                            job_data["error"] = 0.0
                    job_data["chi2dof"] = job_data["iterations"][-1]["chi2dof"]

    return job_data


# @ todo
def grid_score(grid_file: GenericPath) -> float:
    return 42.0
