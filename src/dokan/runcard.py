"""NNLOJET runcard manipulation

collection of helper functions to parse and manipulate NNLOJET runcards
"""

import re
import string
from enum import IntFlag, auto
from os import PathLike
from pathlib import Path


class RuncardTemplate:
    def __init__(self, template: PathLike) -> None:
        self.template: Path = Path(template)

        if not self.template.exists() or not self.template.is_file():
            raise ValueError(f"{self.template} not found?!")

    def fill(self, target: PathLike, **kwargs) -> None:
        RuncardTemplate.fill_template(target, self.template, **kwargs)

    @staticmethod
    def fill_template(runcard: PathLike, template: PathLike, **kwargs):
        """create an NNLOJET runcard from a template.

        parse the runcard and inject variables that can be ppopulated late.
        * run
        * channels
        * channels_region
        * toplevel

        Parameters
        ----------
        runcard : PathLike
            NNLOJET runcard file to write out
        template : PathLike
            The template file to use
        **kwargs
            values for the variables in the template to be substituted.
        """
        with open(template, "r") as t, open(runcard, "w") as f:
            f.write(string.Template(t.read()).substitute(kwargs))


class RuncardBlockFlag(IntFlag):
    # > auto -> integers of: 2^n starting with 1
    PROCESS = auto()
    RUN = auto()
    PARAMETERS = auto()
    SELECTORS = auto()
    HISTOGRAMS = auto()
    HISTOGRAM_SELECTORS = auto()
    COMPOSITE = auto()
    SCALES = auto()
    MULTI_RUN = auto()
    CHANNELS = auto()


class Runcard:
    def __init__(self, runcard: PathLike) -> None:
        self.runcard = Path(runcard)
        if not self.runcard.exists() or not self.runcard.is_file():
            raise ValueError(f"{runcard} does not exist?!")
        self.data: dict = Runcard.parse_runcard(self.runcard)

    def to_tempalte(self, template: PathLike) -> RuncardTemplate:
        Runcard.runcard_to_template(self.runcard, template)
        return RuncardTemplate(template)

    @staticmethod
    def parse_runcard(runcard: PathLike) -> dict:
        """parse an NNLOJET runcard

        Extract settings for a calculation and return as a dictionary
        * process_name
        * job_name

        Parameters
        ----------
        runcard : PathLike
            A NNLOJET runcard file

        Returns
        -------
        dict
            extracted settings
        """
        runcard_data = {}
        runcard_data["histograms"] = ["cross"]
        with open(runcard, "r") as f:
            blk_flag: RuncardBlockFlag = RuncardBlockFlag(0)
            for ln in f:
                # > keep track of the runcard hierarchy & what level/block we're in
                ln_flag: RuncardBlockFlag = RuncardBlockFlag(0)  # accumulate @ end
                for blk in RuncardBlockFlag:
                    if re.match(r"^\s*{}\b".format(blk.name), ln, re.IGNORECASE):
                        ln_flag |= blk
                    if re.match(r"^\s*END_{}\b".format(blk.name), ln, re.IGNORECASE):
                        blk_flag &= ~blk
                        ln = ""  # END_<...> never has options: consume
                        continue

                # > skip "empty" lines (or pure comments)
                ln = re.sub(r"!.*$", "", ln)  # remove comments
                ln = ln.strip()
                if not ln:
                    continue

                # > process_name
                if prc := re.match(r"^\s*PROCESS\s+([^\s!]+)\b", ln, re.IGNORECASE):
                    runcard_data["process_name"] = prc.group(1)

                # > run_name
                if run := re.match(r"^\s*RUN\s+([^\s!]+)\b", ln, re.IGNORECASE):
                    runcard_data["run_name"] = run.group(1)

                # > parse histogram entries
                if sgl := re.match(r"^\s*HISTOGRAMS\s*>\s*([^\s!]+)\b", ln, re.IGNORECASE):
                    runcard_data["histograms_single_file"] = sgl.group(1)
                if RuncardBlockFlag.HISTOGRAMS in blk_flag:
                    skip_flag: RuncardBlockFlag = (
                        RuncardBlockFlag.HISTOGRAM_SELECTORS | RuncardBlockFlag.COMPOSITE
                    )
                    if (skip_flag & blk_flag) or re.match(
                        r"^\s*HISTOGRAM_SELECTORS\b", ln, re.IGNORECASE
                    ):
                        continue
                    if rnm := re.match(r"^\s*(?:[^\s!]+)\s*>\s*([^\s!]+)\b", ln, re.IGNORECASE):
                        runcard_data["histograms"].append(rnm.group(1))
                    elif obs := re.match(r"^\s*([^\s!]+)\b", ln, re.IGNORECASE):
                        runcard_data["histograms"].append(obs.group(1))
                    else:
                        raise RuntimeError(f"could not parse observable in histogram entry: {ln}")

                # > accumulate flag
                blk_flag |= ln_flag
                # blk_flag ^= ln_flag

        if "run_name" not in runcard_data:
            raise RuntimeError("{runcard}: could not find RUN block")
        if "process_name" not in runcard_data:
            raise RuntimeError("{runcard}: could not find PROCESS block")

        return runcard_data

    @staticmethod
    def runcard_to_template(runcard: PathLike, template: PathLike) -> None:
        """create an NNLOJET runcard template file from a generic runcard.

        parse the runcard and inject variables that can be ppopulated late.
        * run
        * channels
        * channels_region
        * toplevel

        Parameters
        ----------
        runcard : PathLike
            A NNLOJET runcard file
        template : PathLike
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
                        raise RuntimeError("invalid line continuation in {}".format(runcard))
                    next_line = next(f)
                    if not re.match(r"^\s*&", next_line):
                        raise RuntimeError("invalid line continuation in {}".format(runcard))
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
                            r"(?<=CHANNELS)\b",
                            "  ${channels_region}",
                            line,
                            re.IGNORECASE,
                        )
                    t.write(line)
                    skiplines = True
                if skiplines and re.match(r"^\s*END_", line, re.IGNORECASE):
                    skiplines = False
                if not skiplines:
                    t.write(line)
            # > append a top-level parameter
            t.write("\n${toplevel}\n")
