import luigi
from sqlalchemy import select

from dokan.db._dbtask import DBDispatch

from .db import Job, DBTask, JobStatus
from .exe import ExecutionMode, ExecutionPolicy


class PreProduction(DBTask):
    part_id: int = luigi.IntParameter()

    def complete(self) -> bool:
        #@todo: placeholder. to be fixed with proper complete condition
        with self.session as session:
            one_job = session.scalars(select(Job).where(Job.part_id == self.part_id)).first()
            if not one_job:
                return False
            else:
                return True

    def run(self):
        print(f"PreProduction: run {self.part_id}")
        with self.session as session:
            last_warmup = session.scalars(
                select(Job).where(Job.part_id == self.part_id).order_by(Job.id.desc())
            ).first()
            if not last_warmup:
                new_warmup = Job(
                    part_id=self.part_id,
                    status=JobStatus.QUEUED,
                    timestamp=0.0,
                    mode=ExecutionMode.WARMUP,
                    policy=ExecutionPolicy.LOCAL,
                    # ncall=,
                    # nit=,
                )
                session.add(new_warmup)
                session.commit()
                # yield DBDispatch(
                #     config=self.config, local_path=self.local_path, id=new_warmup.id
                # )
                print(f"PreProduction: yield job id: {new_warmup.id}")
                yield self.clone(cls=DBDispatch, id=new_warmup.id)
            else:
                # check if the last warmup passes QC criteria
                pass
