import luigi
from enum import IntFlag, auto
from sqlalchemy import select
from pathlib import Path

from .db import Job, DBTask, JobStatus
from .db._dbtask import DBDispatch
from .exe import ExecutionMode, ExecutionPolicy, ExeData


class WarmupFlag(IntFlag):
    # > auto -> integers of: 2^n starting with 1
    RELACC = auto()
    CHI2DOF = auto()
    GRID = auto()
    SCALING = auto()
    MIN_INCREMENT = auto()  ##
    MAX_INCREMENT = auto()  ##
    RUNTIME = auto()  ##


class PreProduction(DBTask):
    part_id: int = luigi.IntParameter()

    def complete(self) -> bool:
        # > check all warmup QC criteria
        if self.append_warmup() > 0:
            return False
        # # > pre-production needs at least one successful production
        # # > that ran with the same policy setting
        # with self.session as session:
        #     success_production = session.scalars(
        #         select(Job)
        #         .where(Job.part_id == self.part_id)
        #         .where(Job.mode == ExecutionMode.PRODUCTION)
        #         .where(Job.policy == self.config["exe"]["policy"])
        #         .where(Job.status.in_(JobStatus.success_list()))
        #     ).first()
        #     if success_production:
        #         return True
        return True

    def append_warmup(self) -> int:
        # > keep track of flags that permit a "warmup done" state
        wflag: WarmupFlag = WarmupFlag(0)

        with self.session as session:
            # > local helper function to extract data from a warmup job
            def get_warmup_data(job: Job) -> ExeData:
                if job.path:
                    exe_data = ExeData(Path(job.path))
                    if job.id not in exe_data["jobs"].keys():
                        raise RuntimeError(
                            f"missing job id {job.id} in data {exe_data!r}"
                        )
                    return exe_data
                raise RuntimeError(f"no data found for {job!r}")

            # > queue up a new warmup job in the database and return job id
            def queue_warmup(ncall: int, niter: int) -> int:
                new_warmup = Job(
                    part_id=self.part_id,
                    mode=ExecutionMode.WARMUP,
                    policy=self.config["exe"]["policy"],
                    status=JobStatus.QUEUED,
                    timestamp=0.0,
                    ncall=ncall,
                    niter=niter,
                )
                session.add(new_warmup)
                session.commit()
                return new_warmup.id

            # > active warmups: return them in order
            # > since `complete` calls this routine, we need to anticipate
            # > calls before completion of active warmup jobs
            active_warmup = session.scalars(
                select(Job)
                .where(Job.part_id == self.part_id)
                .where(Job.mode == ExecutionMode.WARMUP)
                .where(Job.status.in_(JobStatus.active_list()))
                .order_by(Job.id.asc())
            ).first()
            if active_warmup:
                return active_warmup.id

            # > get all previous warmup jobs as a list (all terminated)
            past_warmups = session.scalars(
                select(Job)
                .where(Job.part_id == self.part_id)
                .where(Job.mode == ExecutionMode.WARMUP)
                .where(Job.status.in_(JobStatus.success_list()))
                .order_by(Job.id.desc())
            ).all()

            # > no previous warmup? queue up the first one
            if len(past_warmups) == 0:
                return queue_warmup(
                    self.config["warmup"]["ncall_start"],
                    self.config["warmup"]["niter"],
                )

            # > check increment steps
            if len(past_warmups) >= self.config["warmup"]["min_increment_steps"]:
                wflag |= WarmupFlag.MIN_INCREMENT
            if len(past_warmups) >= self.config["warmup"]["max_increment_steps"]:
                wflag |= WarmupFlag.MAX_INCREMENT
            if WarmupFlag.MAX_INCREMENT in wflag:
                return -int(wflag)

            # > last warmup (LW)
            LW = get_warmup_data(past_warmups[0])
            LW_ntot: int = LW["ncall"] * LW["niter"]
            if abs(LW["error"] / LW["result"]) <= self.config["run"]["target_rel_acc"]:
                wflag |= WarmupFlag.RELACC
            if LW["chi2dof"] < self.config["warmup"]["max_chi2dof"]:
                wflag |= WarmupFlag.CHI2DOF
            # @todo check iterations.txt <-> WarmupFlag.GRID
            if True:
                wflag |= WarmupFlag.GRID

            # > settings for the next warmup (NW) step
            NW_ncall: int = LW["ncall"] * self.config["warmup"]["fac_increment"]
            NW_niter: int = LW["niter"]
            NW_ntot: int = NW_ncall * NW_niter
            NW_time_estimate: float = (
                LW["elapsed_time"] * float(NW_ntot) / float(LW_ntot)
            )
            # > try accommodate runtime limt by reducing iterations
            if NW_time_estimate > self.config["run"]["max_runtime"]:
                NW_niter = int(  # this is a floor
                    float(NW_niter)
                    * self.config["run"]["max_runtime"]
                    / NW_time_estimate
                )
                if NW_niter <= 0:
                    wflag |= WarmupFlag.RUNTIME
                    return -int(wflag)

            # > need to ensure that we have enough increment steps
            if WarmupFlag.MIN_INCREMENT not in wflag:
                return queue_warmup(NW_ncall, NW_niter)

            # > next-to-last warmup (NLW)
            NLW = get_warmup_data(past_warmups[1])
            NLW_ntot: int = NLW["ncall"] * NLW["niter"]
            scaling: float = (LW["error"] ** 2 / float(LW_ntot)) / (
                NLW["error"] ** 2 / float(NLW_ntot)
            )
            if abs(scaling - 1.0) <= self.config["warmup"]["scaling_window"]:
                wflag |= WarmupFlag.SCALING

            # > already reached accuracy and can trust it (chi2dof)
            if WarmupFlag.RELACC in wflag and WarmupFlag.CHI2DOF in wflag:
                return -int(wflag)

            # > warmup has converged
            if (
                WarmupFlag.CHI2DOF in wflag
                and WarmupFlag.GRID in wflag
                and WarmupFlag.SCALING in wflag
            ):
                return -int(wflag)

            # > need more warmup iterations
            return queue_warmup(NW_ncall, NW_niter)

    def run(self):
        print(f"PreProduction: run {self.part_id}")
        if (job_id := self.append_warmup()) > 0:
            yield self.clone(cls=DBDispatch, id=job_id)

