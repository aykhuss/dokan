import luigi
import time
from sqlalchemy import select

from .order import Order
from .db import Part, Job, DBTask, DBInit
from .preproduction import PreProduction


class Entry(DBTask):
    # @todo job options
    # seed start
    # njobs max
    # max concurrent
    order: int = luigi.IntParameter(default=Order.NNLO)
    run_tag: float = luigi.FloatParameter(default=time.time())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print(f"starter: {self.run_tag}: {time.ctime(self.run_tag)}")

    def requires(self):
        # @todo variations to be added here?
        return [
            self.clone(cls=DBInit,
                channels=self.config["process"]["channels"],
            )
            # DBInit(
            #     config=self.config,
            #     local_path=self.local_path,
            #     channels=self.config["process"]["channels"],
            # )
        ]

    def output(self):
        return []

    def complete(self) -> bool:
        return False

    def run(self):
        print("Entry: run")
        # > all pre-productions must complete before we can dispatch production jobs
        preprods = []
        with self.session as session:
            for pt in session.scalars(select(Part)):
                if not pt.active:
                    continue
                print(pt)
                preprod = self.clone(
                    cls=PreProduction,
                    part_id=pt.id,
                )
                # preprod = PreProduction(
                #     config=self.config,
                #     local_path=self.local_path,
                #     part_id=pt.id,
                # )
                preprods.append(preprod)
        print("Entry: yield preprods")
        yield preprods
        print("Entry: complete preprods")
        self.print_job()
