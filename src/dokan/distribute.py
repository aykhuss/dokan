"""Time-distribution optimizer for the production phase.

Pure decision logic: given the production statistics gathered so far and a
runtime budget, decide how much runtime each active Part should receive and
how that runtime splits into jobs.  No Luigi, no SQLAlchemy, no config dict;
`db/_dbproduction.py` gathers the rows, calls in here, and the dispatcher
turns the resulting `TimeAllocation` into queued jobs.

The allocation follows the Euler-Lagrange condition for minimizing the total
variance at fixed total runtime: with `sigma_i` the error of Part `i` and
`T_i` its invested runtime, the optimum distributes new runtime in proportion
to `sigma_i * sqrt(T_i)`.  Parts that would receive a negative share are
excluded and the optimization is repeated until every share is non-negative.
"""

import math
from collections.abc import Sequence
from dataclasses import dataclass, field, replace

from ._types import JobSize

# > adjusted-error tuning: lift errors below the smallest part error towards it,
# > damp outliers logarithmically above `_ADJ_THRESH_MAX` times the median error,
# > and penalize parts that have only a pre-production job so far (their error
# > is a one-sample estimate and cannot be trusted)
_ADJ_THRESH_MAX: float = 7.0
_ADJ_PENALTY: float = 10.0


def safe_rel_error(numerator: float, denominator: float) -> float:
    """Return |numerator / denominator|, or `inf` for a vanishing denominator.

    Used for convergence checks: a vanishing total (including `0/0`) or an
    unknown error must never count as converged.
    """
    return abs(numerator / denominator) if denominator else math.inf


@dataclass(frozen=True)
class PartStats:
    """An active Part's merged result and production statistics for the current policy.

    `tau` and `tau_error` are the mean and standard deviation of seconds per
    event across successful jobs with usable runtime metadata. `pending_events`
    includes active and DONE jobs; `in_flight` only includes active jobs.
    `nfailed` counts this submission's failures since the latest runtime sample.
    """

    id: int
    name: str
    ntot: int
    result: float
    error: float
    tau: float = 0.0
    tau_error: float = 0.0
    sample_count: int = 0
    pending_events: int = 0
    in_flight: bool = False
    nfailed: int = 0

    @property
    def T_merged(self) -> float:
        """Estimated runtime of the merged events: the runtime behind `error`."""
        return self.tau * self.ntot

    @property
    def T_invested(self) -> float:
        """Estimated runtime of the merged and pending events."""
        return self.tau * (self.ntot + self.pending_events)

    @property
    def has_valid_error(self) -> bool:
        return math.isfinite(self.error)


@dataclass(frozen=True)
class DistributeSettings:
    """The config subset that drives the time distribution."""

    target_rel_acc: float
    job_max_runtime: float
    job_fill_max_runtime: bool
    min_number: int
    production_start: JobSize
    # > a Part gets no further jobs after this many failures in a row
    max_consecutive_failures: int

    @classmethod
    def from_config(cls, config: dict) -> "DistributeSettings":
        run: dict = config["run"]
        production: dict = config["production"]
        return cls(
            target_rel_acc=run["target_rel_acc"],
            job_max_runtime=run["job_max_runtime"],
            job_fill_max_runtime=run["job_fill_max_runtime"],
            min_number=production["min_number"],
            production_start=JobSize(ncall=production["ncall_start"], niter=production["niter"]),
            max_consecutive_failures=run["jobs_batch_size"],
        )


@dataclass(frozen=True)
class HealthCheck:
    """Which active Parts cannot take further jobs."""

    # > retries exhausted, or a minimum job exceeds the runtime limit
    stuck: dict[int, str] = field(default_factory=dict)
    # > further production cannot reach the target accuracy: every active Part is
    # > stuck, or a stuck Part has no result yet so the total accuracy stays unknown
    is_stalled: bool = False


@dataclass(frozen=True)
class PartAllocation:
    """The runtime assigned to one Part and how it splits into jobs.

    Only `njobs` decides whether the Part gets jobs.  `job_size` and `T_job`
    always describe the job it *would* get, which also sizes the one-job
    fallback of `prescribe_jobs`.
    """

    # > additional runtime assigned to this Part
    T_opt: float
    # > estimated runtime of one job of `job_size`
    T_job: float
    njobs: int
    job_size: JobSize
    # > runtime invested so far, including jobs that are not merged yet
    T_invested: float
    # > runtime behind the current Part error (merged jobs only)
    T_merged: float
    # > current Part error (`inf` if unknown)
    error: float


@dataclass(frozen=True)
class TimeAllocation:
    """The outcome of one time-distribution optimization.

    `parts` holds the Parts with a usable runtime estimate; all other active
    Parts are listed in `unallocated_part_ids`.  The `tot_*` fields describe
    the accuracy of the total result over all active Parts and `T_target` the
    additional runtime estimated to reach the target accuracy.
    """

    parts: dict[int, PartAllocation] = field(default_factory=dict)
    tot_result: float = 0.0
    tot_error: float = 0.0
    T_target: float = 0.0
    # > active Parts without a usable runtime estimate
    unallocated_part_ids: tuple[int, ...] = ()
    # > unallocated Parts with nothing in flight: only a new job gives them an estimate
    bootstrap_part_ids: tuple[int, ...] = ()
    health: HealthCheck = field(default_factory=HealthCheck)

    @property
    def njobs_total(self) -> int:
        return sum(alloc.njobs for alloc in self.parts.values())

    @property
    def rel_acc(self) -> float:
        """Relative accuracy of the current total result."""
        return safe_rel_error(self.tot_error, self.tot_result)

    @property
    def tot_error_estimate_jobs(self) -> float:
        """Projected total error once the prescribed jobs are merged.

        A Part error only covers the merged runtime, so its variance scales
        with `T_merged / (T_invested + new runtime)`.  Without a runtime
        estimate for every Part there is no projection (`inf`).
        """
        if self.unallocated_part_ids:
            return math.inf
        variance: float = 0.0
        for part in self.parts.values():
            if not math.isfinite(part.error):
                return math.inf
            variance += part.error**2 * part.T_merged / (part.T_invested + part.njobs * part.T_job)
        return math.sqrt(variance)

    @property
    def estimate_rel_acc_jobs(self) -> float:
        """Projected relative accuracy once the prescribed jobs are merged."""
        return safe_rel_error(self.tot_error_estimate_jobs, self.tot_result)


def prescribe_jobs(alloc: TimeAllocation, *, max_jobs: int, batch_size: int) -> TimeAllocation:
    """Limit the job counts to `batch_size` per Part and `max_jobs` in total.

    The projected errors of the returned allocation reflect the final counts.
    """
    counts = {part_id: min(part.njobs, batch_size) for part_id, part in alloc.parts.items()}
    total = sum(counts.values())
    if total > max_jobs:
        counts = {pid: math.ceil(count * max_jobs / total) for pid, count in counts.items()}
        # > round up to retain one-job Parts, then trim the rounding surplus from the largest counts
        for _ in range(sum(counts.values()) - max_jobs):
            part_id = max(counts, key=lambda pid: (counts[pid], -alloc.parts[pid].T_opt))
            counts[part_id] -= 1

    # > always prescribe at least one job: the Part with the largest `T_opt` gets it
    schedulable = [part_id for part_id in alloc.parts if part_id not in alloc.health.stuck]
    if schedulable and not any(counts.values()):
        counts[max(schedulable, key=lambda part_id: alloc.parts[part_id].T_opt)] = 1
    return replace(
        alloc,
        parts={part_id: replace(part, njobs=counts[part_id]) for part_id, part in alloc.parts.items()},
    )


@dataclass
class _PartState:
    """Mutable per-Part state for one optimization."""

    part: PartStats
    allocation_weight: float = 0.0
    T_opt: float = 0.0
    # > the Part receives no new runtime
    excluded: bool = False


def distribute_time(
    parts: Sequence[PartStats], total_t: float, settings: DistributeSettings
) -> TimeAllocation:
    """Distribute `total_t` seconds of additional runtime over the active Parts.

    `parts` are *all* active Parts and all of them enter the accuracy of the
    total with their merged result and error.  Parts without a usable runtime
    estimate are reported in `unallocated_part_ids`, and those among them with
    nothing in flight in `bootstrap_part_ids`.  Stuck Parts receive no runtime.
    `total_t` may be zero or a small dummy value when only the accuracy
    bookkeeping and `T_target` are needed.
    """
    health = _check_health(parts, settings)
    # > a usable runtime estimate needs a runtime sample and invested events (the projection divides by them)
    stats = [
        _PartState(pt, excluded=pt.id in health.stuck)
        for pt in parts
        if pt.sample_count and pt.ntot + pt.pending_events > 0
    ]
    allocated = {s.part.id for s in stats}
    unallocated = [pt for pt in parts if pt.id not in allocated]

    _set_allocation_weights(stats, settings)
    accum_t, total_weight = _optimize_runtime(stats, total_t)

    tot_result: float = sum(pt.result for pt in parts)
    # > E-L estimate of the runtime (beyond what is invested) needed to reach the target accuracy
    target_abs_acc: float = abs(settings.target_rel_acc * tot_result)
    t_target: float = (total_weight / target_abs_acc) ** 2 - accum_t if target_abs_acc > 0.0 else 0.0

    return TimeAllocation(
        parts={s.part.id: _split_into_jobs(s, settings) for s in stats},
        tot_result=tot_result,
        tot_error=math.sqrt(sum(pt.error**2 for pt in parts)),
        T_target=max(0.0, t_target),
        unallocated_part_ids=tuple(pt.id for pt in unallocated),
        bootstrap_part_ids=tuple(
            pt.id for pt in unallocated if not pt.in_flight and pt.id not in health.stuck
        ),
        health=health,
    )


def _check_health(parts: Sequence[PartStats], settings: DistributeSettings) -> HealthCheck:
    """Find the Parts that cannot take another job and explain why."""
    stuck: dict[int, str] = {}
    no_result: bool = False
    niter = settings.production_start.niter
    for pt in parts:
        if pt.nfailed >= settings.max_consecutive_failures:
            reason = (
                f"{pt.nfailed} consecutive production failures"
                f" (limit {settings.max_consecutive_failures}); no further jobs for this part"
            )
        elif pt.tau * niter > settings.job_max_runtime:
            # > a job of one call per iteration is the smallest we can submit
            reason = (
                f"minimum job size ({niter} events) is estimated to take {pt.tau * niter:g}s,"
                f" exceeding job_max_runtime ({settings.job_max_runtime:g}s);"
                " increase job_max_runtime to continue"
            )
        else:
            continue
        if not pt.has_valid_error:
            no_result = True
            reason += "; no result yet; the target accuracy is out of reach"
        stuck[pt.id] = f"part {pt.id} ({pt.name!r}): {reason}"

    return HealthCheck(stuck=stuck, is_stalled=no_result or len(stuck) == len(parts))


def _set_allocation_weights(stats: list[_PartState], settings: DistributeSettings) -> None:
    """Weight each Part by its adjusted merged error times the square root of merged runtime."""
    finite_errors = sorted(s.part.error for s in stats if s.part.has_valid_error and s.part.error > 0.0)
    # > without any positive error there is nothing to floor zero errors with
    pt_min_error = finite_errors[0] if finite_errors else 0.0
    target_error_scale = settings.target_rel_acc * sum(abs(s.part.result) for s in stats)
    pt_max_error = max(finite_errors[-1] if finite_errors else 0.0, target_error_scale) or 1.0
    pt_med_error = finite_errors[len(finite_errors) // 2] if finite_errors else pt_max_error

    damping_threshold: float = _ADJ_THRESH_MAX * pt_med_error
    for s in stats:
        error = s.part.error
        if s.part.has_valid_error:
            adjusted_error = error
            # > enforce non-zero errors: arithmetic mean
            if error < pt_min_error:
                adjusted_error = 0.5 * (error + pt_min_error)
            # > protect against outliers: log damping above the threshold
            if error > damping_threshold:
                adjusted_error = damping_threshold * (1.0 + math.log(error / damping_threshold))
        else:
            # > unknown error: a conservative penalty keeps it schedulable without poisoning the optimum
            adjusted_error = _ADJ_PENALTY * pt_max_error
        # > penalize pre-production only parts
        if _is_min_production(s.part, settings):
            adjusted_error += _ADJ_PENALTY * pt_max_error
        s.allocation_weight = adjusted_error * math.sqrt(s.part.T_merged)


def _optimize_runtime(stats: list[_PartState], total_t: float) -> tuple[float, float]:
    """Set the E-L optimal `T_opt` of every Part.

    Returns the invested runtime and the sum of allocation weights over the
    Parts that receive runtime; both enter the `T_target` estimate.
    """
    # > every pass with a negative share excludes at least one more Part, so the loop terminates
    while True:
        included = [s for s in stats if not s.excluded]
        accum_t = sum(s.part.T_invested for s in included)
        total_weight = sum(s.allocation_weight for s in included)
        no_negative_t_opt: bool = True
        for s in included:
            t_opt: float = 0.0
            if total_weight > 0.0:
                t_opt = (s.allocation_weight / total_weight) * (total_t + accum_t) - s.part.T_invested
            no_negative_t_opt = no_negative_t_opt and t_opt >= 0.0
            s.T_opt = max(t_opt, 0.0)
            s.excluded = t_opt <= 0.0
        if no_negative_t_opt:
            return accum_t, total_weight


def _is_min_production(pt: PartStats, settings: DistributeSettings) -> bool:
    """Only pre-production statistics so far: penalized and capped at one job."""
    return pt.sample_count <= settings.min_number and pt.pending_events <= 0


def _split_into_jobs(s: _PartState, settings: DistributeSettings) -> PartAllocation:
    """Split a Part's optimal runtime into jobs of a schedulable size."""
    tau = s.part.tau
    tau_err = s.part.tau_error
    # > 10 sigma buffer capped at 50%, and the full 50% without any spread (identical samples);
    # > it is this large because the sample is biased: jobs that hit the runtime limit either fail
    # > or keep the estimate
    tau_buf: float = min(10 * tau_err, 0.5 * tau) if tau_err > 0.0 else 0.5 * tau

    # @todo: if a job exceeds the job runtime, should track it in some way so I can
    #        use it to determine this buffer better?

    # > target runtime for one job corrected for buffer
    t_max_job: float = settings.job_max_runtime * (1.0 - tau_buf / tau)
    ntot_max = int(t_max_job / tau)
    njobs: int = 0
    ntot_job = min(settings.production_start.ntot, ntot_max)
    if settings.job_fill_max_runtime:
        njobs = round(s.T_opt / t_max_job)
        ntot_job = ntot_max
    elif s.T_opt > 0.0:
        njobs = int(s.T_opt / t_max_job) + 1
        ntot_job = int(s.T_opt / float(njobs) / tau)
        ntot_job = min(ntot_max, max(settings.production_start.ntot, ntot_job))

    niter = settings.production_start.niter
    # > round up to whole iterations (at least one call each)
    ncall = max(1, (ntot_job + niter - 1) // niter)
    # > rounding up may eat into the buffer but must not exceed the runtime limit (unless ncall = 1)
    ncall = min(ncall, max(1, int(settings.job_max_runtime / (tau * niter))))
    job_size = JobSize(ncall=ncall, niter=niter)

    # > if we inflated the error of a min-production part, we only want to register *one* job
    if _is_min_production(s.part, settings):
        njobs = min(njobs, 1)

    return PartAllocation(
        T_opt=s.T_opt,
        T_job=job_size.ntot * tau,
        njobs=njobs,
        job_size=job_size,
        T_invested=s.part.T_invested,
        T_merged=s.part.T_merged,
        error=s.part.error,
    )
