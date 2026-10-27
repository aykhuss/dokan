"""Database adapter feeding production state into the time-distribution optimizer."""

from itertools import groupby
from statistics import fmean, pstdev

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session

from ..distribute import DistributeSettings, PartStats, TimeAllocation, distribute_time
from ..exe import ExecutionMode
from ._dbtask import DBTask
from ._jobstatus import JobStatus
from ._sqla import Job, Part


def production_remainders(task: DBTask, session: Session) -> tuple[int, float]:
    """Return the remaining production Job count and runtime budgets."""
    njobs_alloc, t_alloc = session.execute(
        select(func.count(Job.id), func.coalesce(func.sum(Job.elapsed_time), 0.0))
        .join(Part)
        .where(Part.active.is_(True))
        .where(Job.run_tag == task.run_tag)
        .where(Job.mode == ExecutionMode.PRODUCTION)
        .where(Job.status.in_(JobStatus.success_list() + JobStatus.active_list()))
    ).one()
    run: dict = task.config["run"]
    return run["jobs_max_total"] - njobs_alloc, run["jobs_max_total"] * run["job_max_runtime"] - t_alloc


def distribute_production_time(task: DBTask, session: Session, total_t: float) -> TimeAllocation:
    """Read one production snapshot and distribute `total_t` seconds over its Parts."""
    successful = JobStatus.success_list()
    # > select plain column values (no ORM objects that could be stale) in a single query
    # > so the merged Part totals and the pending jobs come from the same snapshot
    statement = (
        select(
            Part.id.label("part_id"),
            Part.name,
            Part.ntot,
            Part.result,
            Part.error,
            Job.ncall,
            Job.niter,
            Job.elapsed_time,
            Job.status,
        )
        .outerjoin(
            Job,
            and_(
                Job.part_id == Part.id,
                Job.mode == ExecutionMode.PRODUCTION,
                Job.policy == task.config["exe"]["policy"],
                or_(
                    Job.status.in_(successful + JobStatus.active_list()),
                    and_(Job.status == JobStatus.FAILED, Job.run_tag == task.run_tag),
                ),
            ),
        )
        .where(Part.active.is_(True))
        .order_by(Part.id, Job.id)
    )
    parts: list[PartStats] = []
    for part_id, group in groupby(session.execute(statement), key=lambda row: row.part_id):
        rows = list(group)
        taus: list[float] = []  # > seconds per event of the successful jobs
        pending_events = nfailed = 0
        in_flight = False
        for row in rows:
            status = row.status
            if status is None:  # > Part without any matching job
                continue
            if status == JobStatus.FAILED:
                nfailed += 1
                continue
            ntot = row.ncall * row.niter
            if status in successful and ntot > 0 and row.elapsed_time > 0.0:
                taus.append(row.elapsed_time / ntot)
                nfailed = 0
            if status != JobStatus.MERGED:
                pending_events += ntot
                in_flight |= status not in successful
        head = rows[0]
        parts.append(
            PartStats(
                id=part_id,
                name=head.name,
                ntot=head.ntot,
                result=head.result,
                error=head.error,
                # > average over jobs, not events, to avoid favouring jobs near the runtime limit
                tau=fmean(taus) if taus else 0.0,
                tau_error=pstdev(taus) if taus else 0.0,
                sample_count=len(taus),
                pending_events=pending_events,
                in_flight=in_flight,
                nfailed=nfailed,
            )
        )
    return distribute_time(parts, total_t, DistributeSettings.from_config(task.config))
