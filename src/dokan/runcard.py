"""NNLOJET runcard manipulation

collection of helper functions to parse and manipulate NNLOJET runcards
"""

import re
import string
from enum import IntFlag, auto
from pathlib import Path

from ._types import GenericPath
from .util import template_to_hash, HASH_PATH


class RuncardTemplate:
    def __init__(self, template: GenericPath) -> None:
        self.template: Path = Path(template)

        if not self.template.exists() or not self.template.is_file():
            raise ValueError(f"{self.template} not found?!")

    def fill(self, target: GenericPath, **kwargs) -> None:
        RuncardTemplate.fill_template(target, self.template, **kwargs)

    @staticmethod
    def fill_template(runcard: GenericPath, template: GenericPath, **kwargs):
        """create an NNLOJET runcard from a template.

        parse the runcard and inject variables that can be populated later.
        * run
        * channels
        * channels_region
        * toplevel

        Parameters
        ----------
        runcard : GenericPath
            NNLOJET runcard file to write out
        template : GenericPath
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
    def __init__(self, runcard: GenericPath) -> None:
        self.runcard = Path(runcard)
        if not self.runcard.exists() or not self.runcard.is_file():
            raise ValueError(f"{runcard} does not exist?!")
        self.data: dict = Runcard.parse_runcard(self.runcard)

    def to_template(self, template: GenericPath) -> RuncardTemplate:
        """Writes down the runcard converted into a template file
        and a md5 hash of its content."""
        Runcard.runcard_to_template(self.runcard, template)

        hash_md5 = template_to_hash(template)
        hash_file = Path(template).parent / HASH_PATH
        hash_file.write_text(hash_md5)

        return RuncardTemplate(template)

    @staticmethod
    def parse_runcard(runcard: GenericPath) -> dict:
        """parse an NNLOJET runcard

        Extract settings for a calculation and return as a dictionary
        * process_name
        * job_name
        * histograms
        * PDF(s)

        Parameters
        ----------
        runcard : GenericPath
            A NNLOJET runcard file

        Returns
        -------
        dict
            extracted settings
        """
        runcard_data = {}
        runcard_data["histograms"] = {}
        runcard_data["histograms"]["cross"] = {"nx": 0}
        runcard_data["PDFs"] = []
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

                if pdf := re.match(r"^\s*PDF[12]*\s+=\s+([^\s![]+)\b", ln, re.IGNORECASE):
                    runcard_data["PDFs"].append(pdf.group(1))

                # > parse histogram entries
                if sgl := re.match(r"^\s*HISTOGRAMS\s*>\s*([^\s!]+)\b", ln, re.IGNORECASE):
                    runcard_data["histograms_single_file"] = sgl.group(1)
                if RuncardBlockFlag.HISTOGRAMS in blk_flag:
                    skip_flag: RuncardBlockFlag = (
                        RuncardBlockFlag.HISTOGRAM_SELECTORS | RuncardBlockFlag.COMPOSITE
                    )
                    if not (skip_flag & blk_flag) and not re.match(
                        r"^\s*HISTOGRAM_SELECTORS\b", ln, re.IGNORECASE
                    ):
                        # > extract relevant options of the histogram
                        options: dict = {"nx": 3}  # default
                        if opt := re.match(r".*\bcumulant\s*=\s*([^\s!]+)\b", ln, re.IGNORECASE):
                            options["cumulant"] = int(opt.group(1))
                            if abs(options["cumulant"]) == 1:
                                options["nx"] = 1
                            elif options["cumulant"] != 0:
                                raise RuntimeError(f"unrecognized cumulant option: {ln}")
                        if opt := re.match(r".*\bgrid\s*=\s*([^\s!]+)\b", ln, re.IGNORECASE):
                            options["grid"] = opt.group(1)
                        # > save histogram & option
                        if rnm := re.match(r"^\s*([^\s!]+)\s*>\s*([^\s!]+)\b", ln, re.IGNORECASE):
                            if rnm.group(1) == "cross":
                                options["nx"] = 0
                            runcard_data["histograms"][rnm.group(2)] = options
                        elif obs := re.match(r"^\s*([^\s!]+)\b", ln, re.IGNORECASE):
                            runcard_data["histograms"][obs.group(1)] = options
                        else:
                            raise RuntimeError(
                                f"could not parse observable in histogram entry: {ln}"
                            )

                # > accumulate flag
                blk_flag |= ln_flag
                # blk_flag ^= ln_flag  # <- this would also work for END if we were to delay that accumulation

        if "run_name" not in runcard_data:
            raise RuntimeError(f"{runcard}: could not find RUN block")
        if "process_name" not in runcard_data:
            raise RuntimeError(f"{runcard}: could not find PROCESS block")
        if not runcard_data["PDFs"]:
            raise RuntimeError(f"{runcard}: could not find any PDF set")
        # > observable names with a dot can conflict with how we parse files later
        if any(obs.find(".") != -1 for obs in runcard_data["histograms"].keys()):
            raise RuntimeError(f"{runcard}: observable names with '.' are not supported")

        return runcard_data

    @staticmethod
    def runcard_to_template(runcard: GenericPath, template: GenericPath) -> None:
        """create an NNLOJET runcard template file from a generic runcard.

        parse the runcard and inject variables that can be ppopulated late.
        * run
        * channels
        * channels_region
        * toplevel

        Parameters
        ----------
        runcard : GenericPath
            A NNLOJET runcard file
        template : GenericPath
            The template file to write ou

        Raises
        ------
        RuntimeError
            invalid syntax encountered in runcard.
        """
        kill_matches = [
            # > kill symbols that will be inserted
            re.compile(r"\s*\${sweep}"),
            re.compile(r"\s*\${run}"),
            re.compile(r"\s*\${channels}"),
            re.compile(r"\s*\${channels_region}"),
            re.compile(r"\s*\${toplevel}"),
            # > kill symbols that will be replaced
            re.compile(r"\biseed\s*=\s*\d+\b", re.IGNORECASE),
            re.compile(r"\bwarmup\s*=\s*\d+\[(?:[^\]]+)\]", re.IGNORECASE),
            re.compile(r"\bproduction\s*=\s*\d+\[(?:[^\]]+)\]", re.IGNORECASE),
        ]
        skiplines = False
        with open(runcard, "r") as f, open(template, "w") as t:
            for line in f:
                # > deal with comment lines first (preserve them)
                if re.match(r"^\s*!", line):
                    t.write(line)
                    continue
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
                    t.write("  ${sweep}\n")
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
