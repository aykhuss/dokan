"""Warmup QC assessment and pre-production sizing.

Pure decision logic for the pre-production stage: no Luigi, no SQLAlchemy,
no config dict.  The task layer (`preproduction.py`) gathers job rows and
iteration data, calls into this module, and performs the queuing that the
returned decisions prescribe.
"""

import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import IntFlag, auto


class WarmupFlag(IntFlag):
    """QC criteria and termination reasons behind a warmup decision."""

    # > auto -> integers of: 2^n starting with 1
    # > QC criteria
    RELACC = auto()
    CHI2DOF = auto()
    CONST_ERR = auto()
    GRID = auto()
    SCALING = auto()
    # > increment-step bookkeeping
    MIN_INCREMENT = auto()
    MAX_INCREMENT = auto()
    # > termination reasons outside the QC
    RUNTIME = auto()
    SKIPPED = auto()

    def __str__(self) -> str:
        # > pipe-joined (no brackets: flag strings end up in rich-markup contexts)
        return "|".join(str(flag.name) for flag in WarmupFlag if flag in self)


_NO_FLAGS: WarmupFlag = WarmupFlag(0)


@dataclass(frozen=True)
class JobSize:
    """Validated statistics for one job."""

    ncall: int
    niter: int

    def __post_init__(self) -> None:
        if self.ncall <= 0:
            raise ValueError(f"ncall must be positive, got {self.ncall}")
        if self.niter <= 0:
            raise ValueError(f"niter must be positive, got {self.niter}")

    @property
    def ntot(self) -> int:
        return self.ncall * self.niter


@dataclass(frozen=True)
class WarmupJobQC:
    """The QC-relevant numbers of one successfully terminated warmup job."""

    size: JobSize
    elapsed_time: float
    result: float
    error: float
    chi2dof: float
    id: int = 0

    @classmethod
    def from_row(cls, row) -> "WarmupJobQC":
        """Build from any object carrying the QC fields (e.g. a `Job` DB row)."""
        return cls(
            size=JobSize(ncall=row.ncall, niter=row.niter),
            elapsed_time=row.elapsed_time,
            result=row.result,
            error=row.error,
            chi2dof=row.chi2dof,
            id=row.id,
        )


@dataclass(frozen=True)
class WarmupSettingsQC:
    """The config subset that drives the warmup QC assessment."""

    start: JobSize
    min_increment_steps: int
    max_increment_steps: int
    fac_increment: float
    max_chi2dof: float
    max_err_rel_var: float
    scaling_window: float
    skip_qc: bool
    target_rel_acc: float
    job_max_runtime: float

    @classmethod
    def from_config(cls, config: dict) -> "WarmupSettingsQC":
        warmup: dict = config["warmup"]
        run: dict = config["run"]
        return cls(
            start=JobSize(ncall=warmup["ncall_start"], niter=warmup["niter"]),
            min_increment_steps=warmup["min_increment_steps"],
            max_increment_steps=warmup["max_increment_steps"],
            fac_increment=warmup["fac_increment"],
            max_chi2dof=warmup["max_chi2dof"],
            max_err_rel_var=warmup["max_err_rel_var"],
            scaling_window=warmup["scaling_window"],
            # > transient key (`submit --no-warmup`): absent unless set by the CLI
            skip_qc=warmup.get("skip_qc", False),
            target_rel_acc=run["target_rel_acc"],
            job_max_runtime=run["job_max_runtime"],
        )


@dataclass(frozen=True)
class WarmupCompleteQC:
    """Warmup QC is complete for the current grid."""

    flags: WarmupFlag = _NO_FLAGS


@dataclass(frozen=True)
class WarmupRequiredQC:
    """Another warmup job is required with `size` statistics."""

    size: JobSize
    flags: WarmupFlag = _NO_FLAGS


WarmupAssessmentQC = WarmupCompleteQC | WarmupRequiredQC


@dataclass(frozen=True)
class PreProductionSettings:
    """The config subset that drives the pre-production sizing."""

    start: JobSize
    penalty_wrt_warmup: float
    target_rel_acc: float
    job_max_runtime: float

    @classmethod
    def from_config(cls, config: dict) -> "PreProductionSettings":
        production: dict = config["production"]
        run: dict = config["run"]
        return cls(
            start=JobSize(ncall=production["ncall_start"], niter=production["niter"]),
            penalty_wrt_warmup=production["penalty_wrt_warmup"],
            target_rel_acc=run["target_rel_acc"],
            job_max_runtime=run["job_max_runtime"],
        )


@dataclass(frozen=True)
class SizingDecision:
    """Prescribed size of the next pre-production job (plus a log-worthy condition)."""

    size: JobSize
    warning: str | None = None


def grid_converged() -> bool:
    # @todo check grid  <->  WarmupFlag.GRID
    return True


def assess_warmup(
    past: Sequence[WarmupJobQC],
    iteration_errors: Callable[[], Sequence[float]],
    settings: WarmupSettingsQC,
) -> WarmupAssessmentQC:
    """Assess the warmup QC criteria and decide whether another warmup step is needed.

    `past` are the successfully terminated warmup jobs, most recent first,
    with degenerate rows (`ntot == 0`) excluded by the caller — `ntot` is a
    divisor here; `iteration_errors` supplies the per-iteration errors of the most recent
    warmup — a callable because fetching them costs file IO: it is only
    invoked once the QC can actually conclude on data quality (CONST_ERR),
    never when the decision is already forced (no history, skip,
    MAX_INCREMENT, RUNTIME, or a mandatory increment step outstanding).  An
    empty result means "unavailable": no basis to assess error stability,
    CONST_ERR stays unset.  Assumes `min_increment_steps >= 2`: the SCALING
    criterion needs a next-to-last warmup to compare against.
    """
    wflag: WarmupFlag = WarmupFlag(0)

    # > no previous warmup? prescribe the first one
    if len(past) == 0:
        return WarmupRequiredQC(size=settings.start)

    # > QC skip requested (`submit --no-warmup`): accept the current grid
    if settings.skip_qc:
        return WarmupCompleteQC(flags=wflag | WarmupFlag.SKIPPED)

    # > check increment steps
    if len(past) >= settings.min_increment_steps:
        wflag |= WarmupFlag.MIN_INCREMENT
    if len(past) >= settings.max_increment_steps:
        wflag |= WarmupFlag.MAX_INCREMENT
        return WarmupCompleteQC(flags=wflag)

    # > last warmup (LW)
    LW: WarmupJobQC = past[0]
    # > a vanishing result with a non-zero error (large cancellations) must not divide
    if (LW.result == 0.0 and LW.error == 0.0) or (
        LW.result != 0.0 and abs(LW.error / LW.result) <= settings.target_rel_acc
    ):
        wflag |= WarmupFlag.RELACC
    if LW.chi2dof < settings.max_chi2dof:
        wflag |= WarmupFlag.CHI2DOF
    if grid_converged():
        wflag |= WarmupFlag.GRID

    # > settings for the next warmup (NW) step
    NW_ncall: int = int(LW.size.ncall * settings.fac_increment)
    NW_niter: int = LW.size.niter
    NW_ntot: int = NW_ncall * NW_niter
    NW_time_estimate: float = LW.elapsed_time * float(NW_ntot) / float(LW.size.ntot)
    # > try to accommodate runtime limit by reducing iterations
    if NW_time_estimate > settings.job_max_runtime:
        NW_niter = int(NW_niter * settings.job_max_runtime / NW_time_estimate)
        if NW_niter <= 0:
            return WarmupCompleteQC(flags=wflag | WarmupFlag.RUNTIME)

    next_size = JobSize(ncall=NW_ncall, niter=NW_niter)

    # > need to ensure that we have enough increment steps
    if WarmupFlag.MIN_INCREMENT not in wflag:
        return WarmupRequiredQC(size=next_size, flags=wflag)

    # > only now can the QC conclude on data quality: fetch the iteration
    # > errors (the sole file-IO input) for the error-stability criterion
    if err_list := iteration_errors():
        err_mean: float = sum(err_list) / len(err_list)
        err_stdv: float = math.sqrt(sum((err - err_mean) ** 2 for err in err_list) / len(err_list))
        if err_mean == 0.0 or err_stdv / err_mean < settings.max_err_rel_var:
            wflag |= WarmupFlag.CONST_ERR

    # > next-to-last warmup (NLW): error scaling with statistics
    if len(past) >= 2:
        NLW: WarmupJobQC = past[1]
        scaling: float = 1.0
        if NLW.error != 0.0:
            scaling = (LW.error / NLW.error) * math.sqrt(float(LW.size.ntot) / float(NLW.size.ntot))
        if abs(scaling - 1.0) <= settings.scaling_window:
            wflag |= WarmupFlag.SCALING

    # > already reached accuracy and can trust it (chi2dof)
    if WarmupFlag.RELACC in wflag and WarmupFlag.CHI2DOF in wflag and WarmupFlag.CONST_ERR in wflag:
        return WarmupCompleteQC(flags=wflag)

    # > warmup has converged
    if (
        WarmupFlag.CHI2DOF in wflag
        and WarmupFlag.CONST_ERR in wflag
        and WarmupFlag.GRID in wflag
        and WarmupFlag.SCALING in wflag
    ):
        return WarmupCompleteQC(flags=wflag)

    # > need more warmup iterations
    return WarmupRequiredQC(size=next_size, flags=wflag)


def size_preproduction(last_warmup: WarmupJobQC, settings: PreProductionSettings) -> SizingDecision:
    """Size the pre-production job from the highest-statistics warmup.

    The statistics estimate targets `penalty_wrt_warmup * job_max_runtime`
    (runtime penalty warmup -> production), capped by the statistics needed
    to reach the target accuracy, and floored at `ncall_start`.
    """
    if last_warmup.elapsed_time <= 0.0:
        # > broken/missing runtime metadata (e.g. a log without an "Elapsed time" line):
        # > no basis for a statistics estimate; fall back to the minimal pre-production
        return SizingDecision(
            size=settings.start,
            warning=(
                f"pre-production: warmup {last_warmup.id} has no usable runtime;"
                + " falling back to ncall_start"
            ),
        )

    PP_ntot: int = last_warmup.size.ntot * int(
        settings.penalty_wrt_warmup * settings.job_max_runtime / last_warmup.elapsed_time
    )
    if last_warmup.result != 0.0 and last_warmup.error != 0.0:
        PP_ntot_acc: int = last_warmup.size.ntot * int(
            (last_warmup.error / last_warmup.result / settings.target_rel_acc) ** 2
        )
        PP_ntot = min(PP_ntot, PP_ntot_acc)
    PP_ncall: int = int(PP_ntot) // settings.start.niter
    if PP_ncall < settings.start.ncall:
        PP_ncall = settings.start.ncall

    return SizingDecision(size=JobSize(ncall=PP_ncall, niter=settings.start.niter))


def resize_failed_preproduction(failed: JobSize, settings: PreProductionSettings) -> SizingDecision:
    """Size the retry after a failed pre-production.

    Half the statistics of the failed attempt, floored at `ncall_start`.
    """
    PP_ntot: int = failed.ntot // 2
    PP_ncall: int = max(1, PP_ntot // settings.start.niter)
    warning: str | None = None
    if PP_ncall < settings.start.ncall:
        PP_ncall = settings.start.ncall
        warning = "pre-production failed after reaching minimum ncall"
    return SizingDecision(size=JobSize(ncall=PP_ncall, niter=settings.start.niter), warning=warning)
