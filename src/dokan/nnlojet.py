"""NNLOJET interface

helperfunctions to extract information from NNLOJET
"""

import re
import subprocess

from ._types import GenericPath
from .order import Order

_default_chan_list: dict = {
    "LO": {"string": "LO", "part": "LO", "part_num": 1, "order": 0},
    "R": {"string": "R", "part": "R", "part_num": 1, "order": -1},
    "V": {"string": "V", "part": "V", "part_num": 1, "order": -1},
    "RR": {"string": "RR", "part": "RR", "part_num": 1, "order": -2, "region": "all"},
    "RV": {"string": "RV", "part": "RV", "part_num": 1, "order": -2},
    "VV": {"string": "VV", "part": "VV", "part_num": 1, "order": -2},
}
# @todo complete this list
_proc_has_regions: list = ["1JET", "2JET", "JJ", "ZJ", "WPJ", "WMJ", "HJ", "GJ"]


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
    exe_out = subprocess.run([exe, "-listlumi", proc], capture_output=True, text=True, check=True)
    if exe_out.returncode != 0:
        raise RuntimeError(f"get_lumi: failed calling NNLOJET: {exe_out.stderr}")
    chan_list = dict()
    for line in exe_out.stdout.splitlines():
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
            match_iteration = re.search(r"\(\s*iteration\s+(\d+)\s*\)", line, re.IGNORECASE)
            if match_iteration:
                iteration["iteration"] = int(match_iteration.group(1))
            match_integral = re.search(
                r"\bintegral\s*=\s*(\S+)\s+accum\.\s+integral\s*=\s*(\S+)\b",
                line,
                re.IGNORECASE,
            )
            if match_integral:
                iteration["result"] = float(match_integral.group(1))
                iteration["result_acc"] = float(match_integral.group(2))
            match_stddev = re.search(
                r"\bstd\.\s+dev\.\s*=\s*(\S+)\s+accum\.\s+std\.\s+dev\s*=\s*(\S+)\b",
                line,
                re.IGNORECASE,
            )
            if match_stddev:
                iteration["error"] = float(match_stddev.group(1))
                iteration["error_acc"] = float(match_stddev.group(2))
            match_chi2it = re.search(r"\schi\*\*2/iteration\s*=\s*(\S+)\b", line, re.IGNORECASE)
            if match_chi2it:
                iteration["chi2dof"] = float(match_chi2it.group(1))
                job_data["iterations"].append(iteration)
                iteration = {}
            match_elapsed_time = re.search(
                r"\s*Elapsed\s+time\s*=\s*(\S+)\b\s*(\S+)\b", line, re.IGNORECASE
            )
            if match_elapsed_time:
                unit_time: str = match_elapsed_time.group(2)
                fac_time: float = 1.0
                if unit_time == "seconds":
                    fac_time = 1.0
                elif unit_time == "minutes":
                    fac_time = 60.0
                elif unit_time == "hours":
                    fac_time = 3600.0
                else:
                    raise RuntimeError("unknown time unit")
                job_data["elapsed_time"] = fac_time * float(match_elapsed_time.group(1))
                # > the accumulated results
                job_data["result"] = job_data["iterations"][-1]["result_acc"]
                job_data["error"] = job_data["iterations"][-1]["error_acc"]
                job_data["chi2dof"] = job_data["iterations"][-1]["chi2dof"]

    return job_data


# @ todo
def grid_score(grid_file: GenericPath) -> float:
    return 42.0
