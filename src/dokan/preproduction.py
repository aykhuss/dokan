# SPDX-FileCopyrightText: © 2024-present NNLOJET
#
# SPDX-License-Identifier: MIT

import luigi
from sqlalchemy import select

from .db import Job, DBTask


class PreProduction(DBTask):

    part_id: int = luigi.IntParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        with self.session as session:
            self.last_warmup = session.scalars(
                select(Job).where(Job.part_id == self.part_id).order_by(Job.id.asc())
            ).first()

    def complete(self) -> bool:
        # get last warmup
        return False
