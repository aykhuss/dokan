# SPDX-FileCopyrightText: © 2024-present NNLOJET
#
# SPDX-License-Identifier: MIT

"""NNLOJET runcard manipulation

collection of helper functions to parse and manipulate NNLOJET runcards
"""

import re
import string


def parse_runcard(runcard) -> dict:
    """parse an NNLOJET runcard

    Extract settings for a calculation and return as a dictionary
    * process_name
    * job_name

    Parameters
    ----------
    runcard : FileDescriptorOrPath
        A NNLOJET runcard file

    Returns
    -------
    dict
        extracted settings
    """
    runcard_data = {}
    with open(runcard, "r") as f:
        for line in f:
            # > process_name
            match_proc = re.match(r"^\s*PROCESS\s+([^\s!]+)", line, re.IGNORECASE)
            if match_proc:
                runcard_data["process_name"] = match_proc.group(1)
            # > job_name
            match_job = re.match(r"^\s*RUN\s+([^\s!]+)", line, re.IGNORECASE)
            if match_job:
                runcard_data["job_name"] = match_job.group(1)
    return runcard_data


def make_template(runcard, template):
    """create an NNLOJET runcard template file from a generic runcard.

    parse the runcard and inject variables that can be ppopulated late.
    * run
    * channels
    * channels_region
    * toplevel

    Parameters
    ----------
    runcard : FileDescriptorOrPath
        A NNLOJET runcard file
    template : FileDescriptorOrPath
        The template file to write ou

    Raises
    ------
    RuntimeError
        invalid syntax encountered in runcard.
    """
    kill_matches = [
        # > kill symbol that will be inserted
        re.compile(r"\s*${run}"),
        re.compile(r"\s*${channels}"),
        re.compile(r"\s*${channels_region}"),
        re.compile(r"\s*${toplevel}"),
        # > kill symbols that will be replaced
        re.compile(r"\biseed\s*=\s*\d+\b", re.IGNORECASE),
        re.compile(r"\bwarmup\s*=\s*\d+\[(?:[^\]]+)\]", re.IGNORECASE),
        re.compile(r"\bproduction\s*=\s*\d+\[(?:[^\]]+)\]", re.IGNORECASE),
    ]
    skiplines = False
    with open(runcard, "r") as f, open(template, "w") as t:
        for line in f:
            # > collapse line continuations
            while re.search(r"&", line):
                line = re.sub(r"\s*&\s*(!.*)?$", "", line.rstrip())
                if re.search(r"&", line):
                    raise RuntimeError(
                        "invalid line continuation in {}".format(runcard)
                    )
                next_line = next(f)
                if not re.match(r"^\s*&", next_line):
                    raise RuntimeError(
                        "invalid line continuation in {}".format(runcard)
                    )
                line = line + re.sub(r"^\s*&\s*", " ", next_line)
            # > patch lines to generate a template
            if any(regex.search(line) for regex in kill_matches):
                for regex in kill_matches:
                    line = regex.sub("", line)
                if re.match(r"^\s*$", line):
                    continue
            if re.match(r"^\s*END_RUN", line, re.IGNORECASE):
                t.write("  ${run}\n")
            if re.match(r"^\s*END_CHANNELS", line, re.IGNORECASE):
                t.write("  ${channels}\n")
            if re.match(r"^\s*CHANNELS", line, re.IGNORECASE):
                if re.search(r"\bregion\b", line):
                    line = re.sub(r"\s*region\s*=\s*\w+\b", "${channels_region}", line)
                else:
                    line = re.sub(
                        r"(?<=CHANNELS)\b", "  ${channels_region}", line, re.IGNORECASE
                    )
                t.write(line)
                skiplines = True
            if skiplines and re.match(r"^\s*END_", line, re.IGNORECASE):
                skiplines = False
            if not skiplines:
                t.write(line)
        # > append a top-level parameter
        t.write("\n${toplevel}\n")


def fill_template(runcard, template, **kwargs):
    """create an NNLOJET runcard from a template.

    parse the runcard and inject variables that can be ppopulated late.
    * run
    * channels
    * channels_region
    * toplevel

    Parameters
    ----------
    runcard : FileDescriptorOrPath
        NNLOJET runcard file to write out
    template : FileDescriptorOrPath
        The template file to use
    **kwargs
        values for the variables in the template to be substituted.
    """
    with open(template, "r") as t, open(runcard, "w") as f:
        f.write(string.Template(t.read()).substitute(kwargs))
