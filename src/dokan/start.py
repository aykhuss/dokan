
import luigi
import time
from sqlalchemy import select

from .db import Part, Job, DBTask, DBInit
from .preproduction import PreProduction


class Start(DBTask):
    #@todo job options
    # seed start
    # njobs max
    # max concurrent
    tag: float = luigi.FloatParameter(default=time.time())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print(f"starter: {self.tag}: {time.ctime(self.tag)}")

    def requires(self):
        # @todo variations to be added here?
        return DBInit(
            config=self.config,
            local_path=self.local_path,
            channels=self.config["process"]["channels"],
        )

    def output(self):
        return []

    def complete(self) -> bool:
        return False

    def run(self):
        print("Start: run")
        # preprods = []
        # with self.session as session:
        #     for pt in session.scalars(select(Part)):
        #         print(pt)
        #         preprod = PreProduction(
        #             config=self.config,
        #             local_path=self.local_path,
        #             part_id=pt.id,
        #         )
        #         preprods.append(preprod)
        # yield preprods
