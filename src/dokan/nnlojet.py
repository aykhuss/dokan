"""NNLOJET interface

helperfunctions to extract information from NNLOJET
"""

import re
import subprocess
from os import PathLike

from .order import Order


def get_lumi(exe: PathLike, proc: str) -> dict:
    """get channels for an NNLOJET process

    get the channels with the "part" & "lumi" information collected in groups
    that correspond to independent PDF luminosities of the process.

    Parameters
    ----------
    exe : PathLike
        path to the NNLOJET executable
    proc : str
        NNLOJET process name

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
    exe_out = subprocess.run(
        [exe, "-listlumi", proc], capture_output=True, text=True, check=True
    )
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
    return chan_list


#@todo
def parse_log_file(log_file: PathLike) -> list[dict[str, float]]:
    return [{"a": 1.0}]


#@ todo
def grid_score(grid_file: PathLike) -> float:
    return 42.0
