import luigi
import math
from enum import IntFlag, auto
from sqlalchemy import select
from pathlib import Path

from .db import Job, DBTask, JobStatus
from .db._dbdispatch import DBDispatch
from .db._loglevel import LogLevel
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

    @staticmethod
    def print_flags(flags) -> str:
        ret: str = ""
        if WarmupFlag.RELACC in flags:
            ret += " [RELACC] "
        if WarmupFlag.CHI2DOF in flags:
            ret += " [CHI2DOF] "
        if WarmupFlag.GRID in flags:
            ret += " [GRID] "
        if WarmupFlag.SCALING in flags:
            ret += " [SCALING] "
        if WarmupFlag.MIN_INCREMENT in flags:
            ret += " [MIN_INCREMENT] "
        if WarmupFlag.MAX_INCREMENT in flags:
            ret += " [MAX_INCREMENT] "
        if WarmupFlag.RUNTIME in flags:
            ret += " [RUNTIME] "
        return ret


class PreProduction(DBTask):
    part_id: int = luigi.IntParameter()

    @property
    def resources(self):
        # > each part can only have one active pre-production
        return {f"PreProduction_{self.part_id}": 1}

    def complete(self) -> bool:
        # > check all warmup QC criteria
        if self.append_warmup() > 0:
            return False
        # > make sure, there's one pre-production ready
        if self.append_production() > 0:
            return False
        return True

    def append_warmup(self) -> int:
        # > keep track of flags that permit a "warmup done" state
        wflag: WarmupFlag = WarmupFlag(0)

        with self.session as session:
            # > not needed can get all information from `Job`
            # # > local helper function to extract data from a warmup job
            # def get_warmup_data(job: Job) -> ExeData:
            #     if job.path:
            #         exe_data = ExeData(Path(job.path))
            #         if job.id not in exe_data["jobs"].keys():
            #             raise RuntimeError(
            #                 f"missing job id {job.id} in data {exe_data!r}"
            #             )
            #         return exe_data
            #     raise RuntimeError(f"no data found for {job!r}")

            # > queue up a new warmup job in the database and return job id
            def queue_warmup(ncall: int, niter: int) -> int:
                new_warmup = Job(
                    run_tag=self.run_tag,
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
                .where(Job.run_tag == self.run_tag)
                .where(Job.part_id == self.part_id)
                .where(Job.mode == ExecutionMode.WARMUP)
                .where(Job.status.in_(JobStatus.active_list()))
                .order_by(Job.id.asc())
            ).first()
            if active_warmup:
                # print(f"active warmup: {active_warmup!r}")
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
            LW: Job = past_warmups[0]
            # print(f"LW = {LW!r}")
            # if any(
            #     x is None
            #     for x in [
            #         LW.ncall,
            #         LW.niter,
            #         LW.elapsed_time,
            #         LW.result,
            #         LW.error,
            #         LW.chi2dof,
            #     ]
            # ):
            #     raise RuntimeError(f"missing data in {LW!r}")
            LW_ntot: int = LW.ncall * LW.niter
            if abs(LW.error / LW.result) <= self.config["run"]["target_rel_acc"]:
                wflag |= WarmupFlag.RELACC
            if LW.chi2dof < self.config["warmup"]["max_chi2dof"]:
                wflag |= WarmupFlag.CHI2DOF
            # @todo check iterations.txt <-> WarmupFlag.GRID
            if True:
                wflag |= WarmupFlag.GRID

            # > settings for the next warmup (NW) step
            NW_ncall: int = LW.ncall * self.config["warmup"]["fac_increment"]
            NW_niter: int = LW.niter
            NW_ntot: int = NW_ncall * NW_niter
            NW_time_estimate: float = LW.elapsed_time * float(NW_ntot) / float(LW_ntot)
            # > try accommodate runtime limt by reducing iterations
            if NW_time_estimate > self.config["run"]["job_max_runtime"]:
                NW_niter = (
                    float(NW_niter) * self.config["run"]["job_max_runtime"] // NW_time_estimate
                )
                if NW_niter <= 0:
                    wflag |= WarmupFlag.RUNTIME
                    return -int(wflag)

            # > need to ensure that we have enough increment steps
            if WarmupFlag.MIN_INCREMENT not in wflag:
                return queue_warmup(NW_ncall, NW_niter)

            # > next-to-last warmup (NLW)
            NLW: Job = past_warmups[1]
            NLW_ntot: int = NLW.ncall * NLW.niter
            scaling: float = (LW.error / NLW.error) * math.sqrt(float(LW_ntot) / float(NLW_ntot))

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
            # print(f"PreProduction: append {self.part_id}: {WarmupFlag.print_flags(WarmupFlag(wflag))}")
            return queue_warmup(NW_ncall, NW_niter)

    def append_production(self) -> int:
        with self.session as session:
            # > queue up a new production job in the database and return job id
            def queue_production(ncall: int, niter: int) -> int:
                new_production = Job(
                    run_tag=self.run_tag,
                    part_id=self.part_id,
                    mode=ExecutionMode.PRODUCTION,
                    policy=self.config["exe"]["policy"],
                    status=JobStatus.QUEUED,
                    timestamp=0.0,
                    ncall=ncall,
                    niter=niter,
                )
                session.add(new_production)
                session.commit()
                return new_production.id

            # > active production: return them in order
            # > since `complete` calls this routine, we need to anticipate
            # > calls before completion of active warmup jobs
            active_production = session.scalars(
                select(Job)
                .where(Job.run_tag == self.run_tag)
                .where(Job.part_id == self.part_id)
                .where(Job.mode == ExecutionMode.PRODUCTION)
                .where(Job.policy == self.config["exe"]["policy"])
                .where(Job.status.in_(JobStatus.active_list()))
                .order_by(Job.id.asc())
            ).first()
            if active_production:
                # print(f"active production: {active_production!r}")
                return active_production.id

            # > already have a successful production: done.
            success_production = session.scalars(
                select(Job)
                .where(Job.part_id == self.part_id)
                .where(Job.mode == ExecutionMode.PRODUCTION)
                .where(Job.policy == self.config["exe"]["policy"])
                .where(Job.status.in_(JobStatus.success_list()))
            ).first()
            if success_production:
                return -1

            # > not successful termination => failure
            FPP = session.scalars(
                select(Job)
                .where(Job.part_id == self.part_id)
                .where(Job.mode == ExecutionMode.PRODUCTION)
                .where(Job.policy == self.config["exe"]["policy"])
                .where(Job.status.in_(JobStatus.terminated_list()))
                .order_by(Job.id.desc())
            ).first()
            if FPP:
                # > half the statistics from the last failed pre-production
                PP_ntot: int = (FPP.ncall * FPP.niter) // 2
                PP_ncall: int = PP_ntot // self.config["production"]["niter"]
                if PP_ncall < self.config["production"]["ncall_start"]:
                    self.logger(
                        "pre-production failed after reaching minimum ncall", level=LogLevel.WARN
                    )
                PP_ncall = self.config["production"]["ncall_start"]
                return queue_production(PP_ncall, self.config["production"]["niter"])

            # > queue up a pre-production (PP) with time estimates from the
            # > highest-statistics warumup job we got.
            # > runtime penalty warmup -> production: 1:10
            penalty: float = self.config["production"]["penalty_wrt_warmup"]

            LW = session.scalars(
                select(Job)
                .where(Job.part_id == self.part_id)
                .where(Job.mode == ExecutionMode.WARMUP)
                .where(Job.status.in_(JobStatus.success_list()))
                .order_by(Job.id.desc())
            ).first()
            if not LW:
                raise RuntimeError(f"pre-production: no warmup found for {self.part_id}")
            LW_ntot: int = LW.ncall * LW.niter

            PP_ntot_run: int = LW_ntot * int(
                penalty * self.config["run"]["job_max_runtime"] / LW.elapsed_time
            )
            PP_ntot_acc: int = LW_ntot * int(
                (LW.error / LW.result / self.config["run"]["target_rel_acc"]) ** 2
            )
            PP_ntot: int = min(PP_ntot_run, PP_ntot_acc)
            PP_ncall: int = PP_ntot // self.config["production"]["niter"]
            if PP_ncall < self.config["production"]["ncall_start"]:
                PP_ncall = self.config["production"]["ncall_start"]

            return queue_production(PP_ncall, self.config["production"]["niter"])

    def run(self):
        self.logger(f"PreProduction: run {self.part_id}")
        if (job_id := self.append_warmup()) > 0:
            # self.logger(f"PreProduction: yield {job_id}")
            yield self.clone(cls=DBDispatch, id=job_id)
        self.logger(
            f"PreProduction: warmup done {self.part_id}: {WarmupFlag.print_flags(WarmupFlag(-job_id))}"
        )
        if (job_id := self.append_production()) > 0:
            # self.logger(f"PreProduction: yield {job_id}")
            yield self.clone(cls=DBDispatch, id=job_id)
