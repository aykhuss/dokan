"""dokan job dispatcher

defines the task the dispatches NNLOJET jobs, which also serves the purpose
of re-populating the queue with new jobs based on the current state of the
calculation (available resources, target accuracy, etc.)
"""

import time

import luigi

# from rich.console import Console
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from dokan.db._loglevel import LogLevel

from ..exe import ExecutionMode
from ._dbrunner import DBRunner
from ._dbtask import DBTask
from ._jobstatus import JobStatus
from ._sqla import Job, Part

# _console = Console()


class DBDispatch(DBTask):
    # > dynamic selection: 0
    # > pick a specific `Job` by id: > 0
    # > restrict to specific `Part` by id: < 0 [take abs]
    id: int = luigi.IntParameter(default=0)

    # > in order to be able to create multiple id==0 dispatchers,
    # > need an additional parameter to distinguish them
    _n: int = luigi.IntParameter(default=0)

    # > mode and policy must be set already before dispatch!

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger_prefix: str = (
            self.__class__.__name__ + f"[{self.id}" + (f",{self._n}" if self.id == 0 else "") + "]"
        )
        self.part_id: int = 0  # set in `repoopulate`

    @property
    def resources(self):
        if self.id == 0:
            return super().resources | {"DBDispatch": 1}
        else:
            return super().resources

    priority = 5

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

    def _repopulate(self, session: Session):
        if self.id > 0:
            job: Job = session.get_one(Job, self.id)
            self.part_id = job.part_id

        if self.id < 0:
            self.part_id = abs(self.id)

        if self.id != 0:
            return

        # > get the remaining resources but need to go into the loop
        # > to get the correct state of self.part_id
        njobs_rem, T_rem = self._remainders(session)

        # self._debug(
        #     session, self._logger_prefix + "::repopulate:  " + f"njobs = {njobs_rem}, T = {T_rem}"
        # )

        # > queue up a new production job in the database and return job id's
        def queue_production(part_id: int, opt: dict) -> list[int]:
            nonlocal session
            if opt["njobs"] <= 0:
                return []
            niter: int = self.config["production"]["niter"]
            ncall: int = (opt["ntot_job"] // niter) + 1
            if ncall * niter == 0:
                self._logger(
                    session,
                    f"part {part_id} has ntot={opt['ntot_job']} -> 0 = {ncall} * {niter}",
                    level=LogLevel.WARN,
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
            self._safe_commit(session)
            return [job.id for job in jobs]

        # > build up subquery to get Parts with job counts
        def job_count_subquery(js_list: list[JobStatus]):
            nonlocal session
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
                qbreak = True

            self.part_id = 0  # reset in each loop set @ break

            # > get counters for termination conditions on #queued
            job_count_queued = job_count_subquery([JobStatus.QUEUED])
            job_count_active = job_count_subquery(JobStatus.active_list())
            job_count_success = job_count_subquery(JobStatus.success_list())
            job_min_id_queued = (
                session.query(Job.part_id, func.min(Job.id).label("job_id"))
                .filter(Job.run_tag == self.run_tag)
                .filter(Job.mode == ExecutionMode.PRODUCTION)
                .filter(Job.status.in_([JobStatus.QUEUED]))
                .group_by(Job.part_id)
                .subquery()
            )
            # > get tuples (Part, #queued, #active, #success) ordered by #queued
            sorted_parts = (
                session.query(
                    Part,  # Part.id only?
                    job_count_queued.c.job_count,
                    job_count_active.c.job_count,
                    job_count_success.c.job_count,
                    job_min_id_queued.c.job_id,
                )
                .outerjoin(job_count_queued, Part.id == job_count_queued.c.part_id)
                .outerjoin(job_count_active, Part.id == job_count_active.c.part_id)
                .outerjoin(job_count_success, Part.id == job_count_success.c.part_id)
                .outerjoin(job_min_id_queued, Part.id == job_min_id_queued.c.part_id)
                .filter(Part.active.is_(True))
                # .order_by(job_count_queued.c.job_count.desc())
                .order_by(job_min_id_queued.c.job_id.asc())
                .all()
            )

            # > termination condition based on #queued of individul jobs
            tot_nque: int = 0
            tot_nact: int = 0
            tot_nsuc: int = 0
            for pt, nque, nact, nsuc, jobid in sorted_parts:
                self._debug(session, f"  >> {pt!r} | {nque} | {nact} | {nsuc} | {jobid}")
                nque = nque if nque else 0
                nact = nact if nact else 0
                nsuc = nsuc if nsuc else 0
                tot_nque += nque
                tot_nact += nact
                tot_nsuc += nsuc
                # > implement termination conditions
                if nque >= self.config["run"]["jobs_batch_size"]:
                    qbreak = True
                # > initially, we prefer to increment jobs by 2x
                if nque >= 2 * (nsuc + (nact - nque)):
                    qbreak = True
                # @todo: more?
                # > reset break flag in case below min batch size
                if nque < self.config["run"]["jobs_batch_unit_size"]:
                    qbreak = False
                # > found a part that should be dispatched:
                if qbreak and self.part_id <= 0:
                    # > in case other conditions trigger:
                    # >  pick part with largest # of queued jobs
                    self.part_id = pt.id
                    #  break  # to get `tot_...` right, need to continue the loop
            # > wait until # active jobs drops under max_concurrent with 25% buffer
            if (tot_nact > tot_nque) and (
                tot_nact > 1.25 * self.config["run"]["jobs_max_concurrent"]
            ):
                self._logger(
                    session,
                    self._logger_prefix
                    + "::repopulate:  "
                    + f"{tot_nact} v.s. {self.config['run']['jobs_max_concurrent']} -> sleeping",
                )
                time.sleep(0.1 * self.config["run"]["job_max_runtime"])
                continue
            # > the sole location where we break out of the infinite loop
            if qbreak:
                if self.part_id > 0:
                    pt: Part = session.get_one(Part, self.part_id)
                    self._logger(
                        session,
                        self._logger_prefix + "::repopulate:  " + f"next:  {pt.name}",
                    )
                break

            # > allocate & distribute time for next batch of jobs
            T_next: float = min(
                # self.config["run"]["jobs_batch_size"] * self.config["run"]["job_max_runtime"],
                njobs_rem * self.config["run"]["job_max_runtime"],
                T_rem,
            )
            self._debug(
                session,
                self._logger_prefix
                + "::repopulate:  "
                + f"njobs_rem={njobs_rem}, T_rem={T_rem}, T_next={T_next}",
            )
            opt_dist: dict = self._distribute_time(session, T_next)

            # > interrupt when target accuracy reached
            # @todo does not respect the optimization target yet?
            rel_acc: float = abs(opt_dist["tot_error"] / opt_dist["tot_result"])
            if rel_acc <= self.config["run"]["target_rel_acc"]:
                self._debug(
                    session,
                    self._logger_prefix
                    + "::repopulate:  "
                    + f"rel_acc = {rel_acc} vs. {self.config['run']['target_rel_acc']}",
                )
                qbreak = True
                continue
            # @todo: place to inject the staggered merge settings?

            # > make sure we stay within `njobs` resource limits
            # > by decreasing the number of jobs in proportion to `T_opt`
            lim_njobs: int = min(njobs_rem, self.config["run"]["jobs_max_concurrent"])
            tot_njobs: int = sum(opt["njobs"] for opt in opt_dist["part"].values())
            while tot_njobs > lim_njobs:
                fac: float = (tot_njobs - lim_njobs) / float(tot_njobs)
                tot_njobs = 0  # reset to re-accumulate
                # > keep track of how many jobs were removed
                del_njobs: int = 0
                max_njobs: int = 0
                max_njobs_ipt: int = 0
                max_njobs_T_opt: float = 0.0
                for ipt, opt in opt_dist["part"].items():
                    if opt["njobs"] > 1:  # protect decrementing `njobs=1` (min-production-parts)
                        idel_njobs: int = min(int(fac * opt["njobs"]), opt["njobs"] - 1)
                        opt["njobs"] -= idel_njobs
                        del_njobs += idel_njobs
                    # > re-accumulate total number of jobs
                    tot_njobs += opt["njobs"]
                    # > find job with highest jobs count to use in guaranteed decrement per loop (termination)
                    # > degenerate case: pick the one with *smaller* `T_opt`
                    # > might look odd but want to *decrement* the jobs with smaller `T_opt`
                    if (opt["njobs"] > max_njobs) or (
                        (opt["njobs"] == max_njobs) and (opt["T_opt"] < max_njobs_T_opt)
                    ):
                        max_njobs = opt["njobs"]
                        max_njobs_ipt = ipt
                        max_njobs_T_opt = opt["T_opt"]
                # > make sure every iteration decements so termination is guaranteed
                # > picking max njobs ensures we don't mess up the min-production-parts (njobs==1)
                if del_njobs == 0:
                    opt_dist["part"][max_njobs_ipt]["njobs"] -= 1
                    tot_njobs -= 1
                    del_njobs += 1

            # > register (at least one) job(s)
            tot_T: float = 0.0
            for part_id, opt in sorted(
                opt_dist["part"].items(), key=lambda x: x[1]["T_opt"], reverse=True
            ):
                if tot_njobs == 0:
                    # > at least one job: pick largest T_opt one
                    opt["njobs"] = 1
                    tot_njobs = 1  # trigger only 1st iteration
                # > make sure we don't exceed the batch size (want *continuous* optimization)
                opt["njobs"] = min(opt["njobs"], self.config["run"]["jobs_batch_size"])
                self._debug(session, f"{part_id}: {opt}")
                if opt["njobs"] <= 0:
                    continue
                # > regiser njobs new jobs with ncall,niter and time estime to DB
                ids = queue_production(part_id, opt)
                pt: Part = session.get_one(Part, part_id)
                self._logger(
                    session,
                    self._logger_prefix
                    + "::repopulate:  "
                    + f"register [bold]{len(ids)}[/bold] jobs for {pt.name} [dim](job_ids = {ids})[/dim]",
                )
                tot_T += opt["njobs"] * opt["T_job"]

            # > commit & update remaining resources for next iteration
            self._safe_commit(session)
            njobs_rem -= tot_njobs
            T_rem -= tot_T

            estimate_rel_acc: float = abs(
                opt_dist["tot_error_estimate_jobs"] / opt_dist["tot_result"]
            )
            if estimate_rel_acc <= self.config["run"]["target_rel_acc"]:
                qbreak = True
                continue

    def run(self):
        with self.session as session:
            self._debug(session, self._logger_prefix + "::run:  " + f"part_id = {self.part_id}")
            self._repopulate(session)

            # > queue empty and no job added in `repopulate`: we're done
            if self.part_id <= 0:
                return

            # > get the queue
            stmt = self.select_job.where(Job.status == JobStatus.QUEUED)
            if self.id == 0:
                stmt = stmt.where(Job.part_id == self.part_id)
            # > compile batch in `id` order
            jobs: list[Job] = [*session.scalars(stmt.order_by(Job.id.asc())).all()]
            if jobs:
                # > most recent entry [-1] sets overall statistics
                for j in jobs:
                    j.ncall = jobs[-1].ncall
                    j.niter = jobs[-1].niter
                    j.elapsed_time = jobs[-1].elapsed_time
                if (
                    self.id == 0
                ):  # only for production dispatch @todo think about warmup & pre-production
                    # > try to exhaust the batch with multiples of the batch unit size
                    nbatch_curr: int = min(len(jobs), self.config["run"]["jobs_batch_size"])
                    nbatch_unit: int = self.config["run"]["jobs_batch_unit_size"]
                    nbatch: int = (nbatch_curr // nbatch_unit) * nbatch_unit
                    jobs = jobs[:nbatch]

            # > set seeds for the jobs to prepare for a dispatch
            if jobs:
                # > get last job that has a seed assigned to it
                last_job = session.scalars(
                    select(Job)
                    .where(Job.part_id == self.part_id)
                    .where(Job.mode == jobs[0].mode)
                    .where(Job.seed.is_not(None))
                    .where(Job.seed > self.config["run"]["seed_offset"])
                    # @todo not good enough, need a max to shield from another batch-job starting at larger value of seed?
                    # determine upper bound by the max number of jobs? -> seems like a good idea
                    .order_by(Job.seed.desc())
                ).first()
                if last_job and last_job.seed:
                    self._debug(
                        session,
                        self._logger_prefix + "::run:  " + f"{self.id} last job:  {last_job!r}",
                    )
                    seed_start: int = last_job.seed + 1
                else:
                    seed_start: int = self.config["run"]["seed_offset"] + 1

                for iseed, job in enumerate(jobs, seed_start):
                    job.seed = iseed
                    job.status = JobStatus.DISPATCHED
                self._safe_commit(session)

                # > time to dispatch Runners
                pt: Part = session.get_one(Part, self.part_id)
                self._logger(
                    session,
                    self._logger_prefix
                    + "::run:  "
                    + f"submitting {pt.name} jobs with "
                    + (
                        f"seeds: {jobs[0].seed}-{jobs[-1].seed}"
                        if len(jobs) > 1
                        else f"seed: {jobs[0].seed}"
                    ),
                )
                yield self.clone(cls=DBRunner, ids=[job.id for job in jobs], part_id=self.part_id)
