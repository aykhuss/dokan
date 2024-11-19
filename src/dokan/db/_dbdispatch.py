"""dokan job dispatcher

defines the task the dispatches NNLOJET jobs, which also serves the purpose
of re-populating the queue with new jobs based on the current state of the
calculation (available resources, target accuracy, etc.)
"""

import luigi
from sqlalchemy import func, select

from ..exe import ExecutionMode
from ._dbrunner import DBRunner
from ._dbtask import DBTask
from ._jobstatus import JobStatus
from ._sqla import Job, Part


class DBDispatch(DBTask):
    # > inactive selection: 0
    # > pick a specific `Job` by id: > 0
    # > restrict to specific `Part` by id: < 0 [take abs]
    id: int = luigi.IntParameter(default=0)

    # > in order to be able to create multiple id==0 dispatchers,
    # need an additional parameter to distinguish them
    _n: int = luigi.IntParameter(default=0)

    # > mode and policy must be set already before dispatch!

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.part_id: int = 0  # set in `repoopulate`

    @property
    def resources(self):
        if self.id == 0:
            return {"DBDispatch": 1}
        else:
            return None

    # priority = 10

    @property
    def select_job(self):
        # > define the selector for the jobs based on the id that was passed & filter by the run_tag
        slct = select(Job).where(Job.run_tag == self.run_tag)
        if self.id > 0:
            return slct.where(Job.id == self.id)
        elif self.id < 0:
            return slct.where(Job.part_id == abs(self.id))
        else:
            return slct

    def complete(self) -> bool:
        with self.session as session:
            if (
                session.scalars(self.select_job.where(Job.status == JobStatus.QUEUED)).first()
                is not None
            ):
                return False
            return True

    def repopulate(self):
        if self.id > 0:
            with self.session as session:
                job: Job = session.get_one(Job, self.id)
                self.part_id = job.part_id

        if self.id < 0:
            self.part_id = abs(self.id)

        if self.id != 0:
            return

        njobs_rem, T_rem = self.remainders()
        if njobs_rem <= 0 or T_rem <= 0.0:
            self.debug(
                f"DBDispatch[{self.id},{self._n}]::repopulate: resources depleted:  "
                + f"njobs = {njobs_rem}, T = {T_rem}"
            )
            return

        with self.session as session:
            self.debug(
                f"DBDispatch[{self.id},{self._n}]::repopulate:  "
                + f"njobs = {njobs_rem}, T = {T_rem}"
            )

            # > queue up a new production job in the database and return job id's
            def queue_production(part_id: int, opt: dict) -> list[int]:
                if opt["njobs"] <= 0:
                    return []
                niter: int = self.config["production"]["niter"]
                ncall: int = opt["ntot_job"] // niter
                if ncall * niter == 0:
                    self.logger(
                        f"part {part_id} has ntot={opt['ntot_job']} -> 0 = {ncall} * {niter}"
                    )
                    # ncall = self.config["production"]["ncall_start"]
                    return []
                jobs: list[Job] = [
                    Job(
                        run_tag=self.run_tag,
                        part_id=part_id,
                        mode=ExecutionMode.PRODUCTION,
                        policy=self.config["exe"]["policy"],
                        status=JobStatus.QUEUED,
                        timestamp=0.0,
                        ncall=ncall,
                        niter=niter,
                        elapsed_time=opt["T_job"],  # a time estimate
                    )
                    for _ in range(opt["njobs"])
                ]
                session.add_all(jobs)
                session.commit()
                return [job.id for job in jobs]

            # > build up subquery to get Parts with job counts
            def job_count_subquery(js_list: list[JobStatus]):
                return (
                    session.query(Job.part_id, func.count(Job.id).label("job_count"))
                    .filter(Job.run_tag == self.run_tag)
                    .filter(Job.mode == ExecutionMode.PRODUCTION)
                    .filter(Job.status.in_(js_list))
                    .group_by(Job.part_id)
                    .subquery()
                )

            # > populate until some termination condition is reached
            qbreak: bool = False  # control where we break out (to set self.part_id)
            while True:
                if njobs_rem <= 0 or T_rem <= 0.0:
                    return

                self.part_id = 0  # reset in each loop set @ break

                # > get counters for temrination conditions on #queued
                job_count_queued = job_count_subquery([JobStatus.QUEUED])
                job_count_active = job_count_subquery(JobStatus.active_list())
                job_count_success = job_count_subquery(JobStatus.success_list())
                # > get tuples (Part, #queued, #active, #success) ordered by #queued
                sorted_parts = (
                    session.query(
                        Part,  # Part.id only?
                        job_count_queued.c.job_count,
                        job_count_active.c.job_count,
                        job_count_success.c.job_count,
                    )
                    .outerjoin(job_count_queued, Part.id == job_count_queued.c.part_id)
                    .outerjoin(job_count_active, Part.id == job_count_active.c.part_id)
                    .outerjoin(job_count_success, Part.id == job_count_success.c.part_id)
                    .filter(Part.active.is_(True))
                    .order_by(job_count_queued.c.job_count.desc())
                    .all()
                )

                # > termination condition based on #queued of individul jobs
                for pt, nque, nact, nsuc in sorted_parts:
                    self.debug(f"  >> {pt!r} | {nque} | {nact} | {nsuc}")
                    if not nque:
                        continue
                    # > implement termination conditions
                    if nque >= self.config["run"]["jobs_batch_size"]:
                        qbreak = True
                    nsuc = nsuc if nsuc else 0
                    nact = nact if nact else 0
                    # > initially, we prefer to increment jobs by 2x
                    if nque >= 2 * (nsuc + (nact - nque)):
                        qbreak = True
                    # @todo: more?
                    # > found a part that should be dispatched:
                    if qbreak:
                        # > in case other contitions trigger:
                        # >  pick part with largest # of queued jobs
                        self.part_id = pt.id
                        break

                # > the sole location where we break out of the infinite loop
                if qbreak:
                    self.logger(
                        f"DBDispatch[{self.id},{self._n}]::repopulate:  "
                        + f"next in line is part_id = {self.part_id}"
                    )
                    break

                # > allocate & distribute time for next batch of jobs
                T_next: float = min(
                    self.config["run"]["jobs_batch_size"] * self.config["run"]["job_max_runtime"],
                    njobs_rem * self.config["run"]["job_max_runtime"],
                    T_rem,
                )
                opt_dist: dict = self.distribute_time(T_next)

                # > interrupt when target accuracy reached
                rel_acc: float = abs(opt_dist["tot_error"] / opt_dist["tot_result"])
                if rel_acc <= self.config["run"]["target_rel_acc"]:
                    self.debug(
                        f"DBDispatch[{self.id},{self._n}]::repopulate:  "
                        + f'rel_acc = {rel_acc} vs. {self.config["run"]["target_rel_acc"]}'
                    )
                    qbreak = True
                    continue

                # > make sure we stay within `njobs` resource limits
                while (
                    tot_njobs := sum(opt["njobs"] for opt in opt_dist["part"].values())
                ) > njobs_rem:
                    # > decrement in steps of min/2
                    dec_njobs: int = (
                        min(opt["njobs"] for opt in opt_dist["part"].values() if opt["njobs"] > 0)
                        // 2
                        + 1
                    )
                    for opt in opt_dist["part"].values():
                        if opt["njobs"] > 0:
                            opt["njobs"] -= dec_njobs

                # > register (at least one) job(s)
                tot_T: float = 0.0
                for part_id, opt in sorted(
                    opt_dist["part"].items(), key=lambda x: x[1]["T_opt"], reverse=True
                ):
                    if tot_njobs == 0:
                        # > at least one job: pick largest T_opt one
                        opt["njobs"] = 1
                        tot_njobs = 1  # trigger only 1st iteration
                    self.debug(f"{part_id}: {opt}")
                    if opt["njobs"] <= 0:
                        continue
                    # > regiser njobs new jobs with ncall,niter and time estime to DB
                    ids = queue_production(part_id, opt)
                    self.logger(
                        f"DBDispatch[{self.id},{self._n}]::repopulate:  "
                        + f"part_id = {part_id}:  (#:{len(ids)}) {ids}"
                    )
                    tot_T += opt["njobs"] * opt["T_job"]

                # > commit & update remaining resources for next iteration
                session.commit()
                njobs_rem -= tot_njobs
                T_rem -= tot_T

                estimate_rel_acc: float = abs(
                    opt_dist["tot_error_estimate_jobs"] / opt_dist["tot_result"]
                )
                if estimate_rel_acc <= self.config["run"]["target_rel_acc"]:
                    qbreak = True
                    continue

    def run(self):
        self.repopulate()

        if self.part_id <= 0:
            # > should never get here
            raise RuntimeError(f"DBDispatch::run: invalid part_id = {self.part_id}!")

        with self.session as session:
            # > get the queue
            stmt = self.select_job.where(Job.status == JobStatus.QUEUED)
            if self.id == 0:
                stmt = stmt.where(Job.part_id == self.part_id)
            # > compile batch in `id` order
            job = session.scalars(stmt.order_by(Job.id.asc())).first()
            if job:
                # @todo set seeds here! (batch size rounding and ordering)
                # > get last job that has a seed assigned to it
                last_job = session.scalars(
                    select(Job)
                    .where(Job.part_id == job.part_id)
                    .where(Job.mode == job.mode)
                    .where(Job.seed.is_not(None))
                    .where(Job.seed > self.config["run"]["seed_offset"])
                    # @todo not good enough, need a max to shield from anothe batch-jobstarting at larger value of seed?
                    # determine upper bound by the max number of jobs? -> seems like a good idea
                    .order_by(Job.seed.desc())
                ).first()
                if last_job:
                    # print(f"{self.id} last job:\n>  {last_job!r}")
                    seed_start: int = last_job.seed + 1
                else:
                    seed_start: int = self.config["run"]["seed_offset"] + 1
                job.seed = seed_start
                job.status = JobStatus.DISPATCHED
                session.commit()
                # if self.id == 0:
                #     self.decrease_running_resources({"DBDispatch": 1})
                self.logger(f"DBDispatch[{self.id},{self._n}]::run:  submitting {job!r}")
                yield self.clone(cls=DBRunner, id=job.id)
