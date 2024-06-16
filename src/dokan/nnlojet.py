"""NNLOJET interface

helperfunctions to extract information from NNLOJET
"""

import re
import subprocess


# def which(program):
#
#   def is_exe(fpath):
#     return os.path.isfile(fpath) and os.access(fpath, os.X_OK)
#
#   fpath, fname = os.path.split(program)
#   if fpath:
#     if is_exe(program):
#       return program
#   else:
#     for path in os.environ["PATH"].split(os.pathsep):
#       exe_file = os.path.join(path, program)
#       if is_exe(exe_file):
#         return exe_file
#   return None



def get_lumi(exe: str, proc: str) -> dict:
    """get channels (part & lumi split)

    Args:
        exe (str): path to NNLOJET executable
        proc (str): NNLOJET process name

    Returns:
        dict: label(RRa_42) -> {part(RR), [region(a),] part_id(42), string("1 2 3 ...")}

    Raises:
        RuntimeError: parsing error
    """
    exe_out = subprocess.run([exe, "-listlumi", proc],
                             capture_output=True,
                             text=True)
    chan_list = {}
    for line in exe_out.stdout.splitlines():
        if not re.search(r' ! channel: ', line):
            continue
        label = None
        chan = dict()
        match = re.match(r'^\s*(\w+)\s+(.*)$', line)
        if match:
            label = match.group(1)
            chan['string'] = match.group(2)
        else:
            raise RuntimeError("couldn't parse channel line")
        match = re.match(r'^([^_]+)_(\d+)$', label)
        if match:
            chan['part'] = match.group(1)
            chan['part_id'] = int(match.group(2))
            if chan['part'][-1] == 'a' or chan['part'][-1] == 'b':
                chan['region'] = chan['part'][-1]
                chan['part'] = chan['part'][:-1]
        else:
            raise RuntimeError("couldn't parse channel line")
        chan_list[label] = chan
    return chan_list
