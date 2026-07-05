import math
from enum import IntFlag, auto

import luigi
from sqlalchemy import select
from sqlalchemy.orm import Session

from .db import DBTask, Job, JobStatus
from .db._dbdispatch import DBDispatch
from .db._dbresurrect import DBResurrect
from .db._loglevel import LogLevel
from .exe import ExecutionMode
from .exe._exe_data import ExeData


class WarmupFlag(IntFlag):
    # > auto -> integers of: 2^n starting with 1
    RELACC = auto()
    CHI2DOF = auto()
    CONST_ERR = auto()
    GRID = auto()
    SCALING = auto()
    MIN_INCREMENT = auto()
    MAX_INCREMENT = auto()
    RUNTIME = auto()
    FROZEN = auto()

    @staticmethod
    def print_flags(flags) -> str:
        ret: str = ""
        if WarmupFlag.FROZEN in flags:
            ret += " [FROZEN] "
        if WarmupFlag.RELACC in flags:
            ret += " [RELACC] "
        if WarmupFlag.CONST_ERR in flags:
            ret += " [CONST_ERR] "
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
    part_id: int = luigi.IntParameter()  # type: ignore[assignment]

    priority = 150

    @property
    def resources(self):  # type: ignore
        # > each part can only have one active pre-production
        return super().resources | {f"PreProduction_{self.part_id}": 1}

    @property
    def _logger_prefix(self) -> str:
        # > lazy: the part-name lookup must not happen at construction time
        return self.__class__.__name__ + f"[{self._part_name(self.part_id)}]"

    def complete(self) -> bool:
        with self.session as session:
            # > check all warmup QC criteria
            if self._append_warmup(session) > 0:
                return False
            # > make sure, there's one pre-production ready
            if self._append_production(session) > 0:
                return False
        return True

    def _append_warmup(self, session: Session) -> int:
        # > keep track of flags that permit a "warmup done" state
        wflag: WarmupFlag = WarmupFlag(0)

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
            nonlocal session
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
            self._safe_commit(session)
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

        # > need at least one warmup; then check for the frozen status
        if self.config["warmup"]["frozen"]:
            wflag |= WarmupFlag.FROZEN
            return -int(wflag)

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
        # > a vanishing result with a non-zero error (large cancellations) must not divide
        if (LW.result == 0.0 and LW.error == 0.0) or (
            LW.result != 0.0
            and abs(LW.error / LW.result) <= self.config["run"]["target_rel_acc"]
        ):
            wflag |= WarmupFlag.RELACC
        if LW.chi2dof < self.config["warmup"]["max_chi2dof"]:
            wflag |= WarmupFlag.CHI2DOF
        # > QC measures that require the ExeData information; a job without parsed
        # > iterations gives no basis to assess error stability: leave CONST_ERR unset
        exe_data: ExeData = ExeData(self._local(LW.rel_path))
        job_data: dict | None = exe_data["jobs"].get(LW.id)
        err_list: list[float] = [it["error"] for it in job_data.get("iterations", [])] if job_data else []
        if err_list:
            err_mean: float = sum(err_list) / len(err_list)
            err_stdv: float = math.sqrt(sum((err - err_mean) ** 2 for err in err_list) / len(err_list))
            if err_mean == 0.0 or err_stdv / err_mean < self.config["warmup"]["max_err_rel_var"]:
                wflag |= WarmupFlag.CONST_ERR
        # @todo check iterations.txt <-> WarmupFlag.GRID
        if True:
            wflag |= WarmupFlag.GRID

        # > settings for the next warmup (NW) step
        NW_ncall: int = LW.ncall * self.config["warmup"]["fac_increment"]
        NW_niter: int = LW.niter
        NW_ntot: int = NW_ncall * NW_niter
        NW_time_estimate: float = LW.elapsed_time * float(NW_ntot) / float(LW_ntot)
        # > try to accommodate runtime limit by reducing iterations
        if NW_time_estimate > self.config["run"]["job_max_runtime"]:
            NW_niter = int(NW_niter * self.config["run"]["job_max_runtime"] / NW_time_estimate)
            if NW_niter <= 0:
                wflag |= WarmupFlag.RUNTIME
                return -int(wflag)

        # > need to ensure that we have enough increment steps
        if WarmupFlag.MIN_INCREMENT not in wflag:
            return queue_warmup(NW_ncall, NW_niter)

        # > next-to-last warmup (NLW)
        NLW: Job = past_warmups[1]
        NLW_ntot: int = NLW.ncall * NLW.niter
        scaling: float = 1.0
        if NLW.error != 0.0:
            scaling: float = (LW.error / NLW.error) * math.sqrt(float(LW_ntot) / float(NLW_ntot))

        if abs(scaling - 1.0) <= self.config["warmup"]["scaling_window"]:
            wflag |= WarmupFlag.SCALING

        # > already reached accuracy and can trust it (chi2dof)
        if WarmupFlag.RELACC in wflag and WarmupFlag.CHI2DOF in wflag and WarmupFlag.CONST_ERR in wflag:
            return -int(wflag)

        # > warmup has converged
        if (
            WarmupFlag.CHI2DOF in wflag
            and WarmupFlag.CONST_ERR in wflag
            and WarmupFlag.GRID in wflag
            and WarmupFlag.SCALING in wflag
        ):
            return -int(wflag)

        # > need more warmup iterations
        # print(f"PreProduction: append {self.part_id}: {WarmupFlag.print_flags(WarmupFlag(wflag))}")
        return queue_warmup(NW_ncall, NW_niter)

    def _append_production(self, session: Session) -> int:
        # > queue up a new production job in the database and return job id
        def queue_production(ncall: int, niter: int) -> int:
            nonlocal session
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
            self._safe_commit(session)
            return new_production.id

        # > complete production:
        # > if there's one complete, we're not in pre-production stage!
        complete_production = session.scalars(
            select(Job)
            .where(Job.part_id == self.part_id)
            .where(Job.mode == ExecutionMode.PRODUCTION)
            .where(Job.policy == self.config["exe"]["policy"])
            .where(Job.status.in_(JobStatus.success_list()))
            .order_by(Job.id.asc())
        ).first()
        if complete_production:
            return -1

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
            return active_production.id

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
                self._logger(
                    session,
                    "pre-production failed after reaching minimum ncall",
                    level=LogLevel.WARN,
                )
            PP_ncall = self.config["production"]["ncall_start"]
            return queue_production(PP_ncall, self.config["production"]["niter"])

        # > queue up a pre-production (PP) with time estimates from the
        # > highest-statistics warmup job we got.
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

        if LW.elapsed_time <= 0.0:
            # > broken/missing runtime metadata (e.g. a log without an "Elapsed time" line):
            # > no basis for a statistics estimate; fall back to the minimal pre-production
            self._logger(
                session,
                f"pre-production: warmup {LW.id} has no usable runtime; falling back to ncall_start",
                level=LogLevel.WARN,
            )
            return queue_production(
                self.config["production"]["ncall_start"], self.config["production"]["niter"]
            )

        PP_ntot: int = LW_ntot * int(penalty * self.config["run"]["job_max_runtime"] / LW.elapsed_time)
        if LW.result != 0.0 and LW.error != 0.0:
            PP_ntot_acc: int = LW_ntot * int(
                (LW.error / LW.result / self.config["run"]["target_rel_acc"]) ** 2
            )
            PP_ntot = min(PP_ntot, PP_ntot_acc)
        PP_ncall: int = PP_ntot // self.config["production"]["niter"]
        if PP_ncall < self.config["production"]["ncall_start"]:
            PP_ncall = self.config["production"]["ncall_start"]

        return queue_production(PP_ncall, self.config["production"]["niter"])

    def _dispatch_then_resurrect(self, job_id: int, stage: str):
        """Yield the bounded dispatch of `job_id`, then a resurrection when reached inline.

        Luigi only continues past a dynamic `yield` in the same pass when the
        yielded task was already complete at yield time; for a bounded dispatch
        that means `job_id` is no longer QUEUED, i.e. an already-active job from
        a previous run that must be resurrected.  Its `rel_path` is re-read
        *after* the yield: dispatch completion only guarantees DISPATCHED, and
        a concurrent `DBRunner` may assign the path at any moment — a job that
        still has none cannot be resurrected and is a loud error.
        """
        yield self.clone(cls=DBDispatch, id=job_id)
        with self.session as session:
            self._logger(
                session,
                self._logger_prefix + f"::run:  resurrect {stage} [dim](job_id = {job_id})[/dim]",
            )
            rel_path: str | None = session.get_one(Job, job_id).rel_path
        if rel_path is None:
            raise RuntimeError(self._logger_prefix + f"::run:  job {job_id} has no path to resurrect")
        yield self.clone(cls=DBResurrect, rel_path=rel_path)

    def run(self):  # type: ignore[override]
        """Drive the warmup/pre-production state machine.

        Every `yield` sits outside a DB session: Luigi abandons the generator on
        suspension (the `with` block would never exit and leak the session).
        """
        # > warmup stage
        with self.session as session:
            self._part_name(self.part_id, session)  # prime the log-prefix cache
            self._logger(session, self._logger_prefix + "::run")
            job_id: int = self._append_warmup(session)
            if job_id > 0:
                self._logger(
                    session, self._logger_prefix + f"::run:  yield warmup [dim](job_id = {job_id})[/dim]"
                )
        if job_id > 0:
            yield from self._dispatch_then_resurrect(job_id, "warmup")
        assert job_id < 0, self._logger_prefix + f"::run:  warmup job_id = {job_id} < 0 expected!"

        # > pre-production stage
        with self.session as session:
            self._logger(
                session,
                self._logger_prefix
                + "::run:  warmup done"
                + f" [dim]{WarmupFlag.print_flags(WarmupFlag(-job_id))}[/dim]",
            )
            job_id = self._append_production(session)
            if job_id > 0:
                self._logger(
                    session,
                    self._logger_prefix + f"::run:  yield pre-production [dim](job_id = {job_id})[/dim]",
                )
        if job_id > 0:
            yield from self._dispatch_then_resurrect(job_id, "pre-production")
