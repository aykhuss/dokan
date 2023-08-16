import string
import os
import shutil
import random
import time
import json
import math

import luigi
from . import *


class Production(Task):

    _file_res: str = "production.json"
    _file_tmp: str = "production.tmp"

    channel: str = luigi.Parameter()
    iseed: int = luigi.IntParameter()

    @property
    def resources(self) -> dict:
        """limit to one production in `run` at a time
        """
        return {"production_" + '_'.join(map(str, self.local_path)): 1}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print("Production: {}".format(id(self)))

        if self.channel not in self.config["process"]["channels"]:
            raise RuntimeError("Production: unknown channel: {}".format(
                self.channel))
        #> dict of tuples: { 'p<iseed>': [ dict(exe_args), [None|dict(result)] ] }
        self._data: dict = dict()
        self.load_data()

        # #> not done yet: res -> tmp
        # if (not self.complete()
        #         and os.path.exists(self._local(Production._file_res))):
        #     shutil.move(self._local(Production._file_res),
        #                 self._local(Production._file_tmp))

    def requires(self):
        return Warmup(config=self.config,
                      local_path=self.local_path,
                      channel=self.channel)

    def output(self):
        return luigi.LocalTarget(self._local(Production._file_res))

    def complete(self):
        idir: str = Production._idir(self.iseed)
        if idir in self._data:
            return (self._data[idir][1] is not None)
        return False

    @staticmethod
    def _idir(iseed: int) -> str:
        return "p{}".format(iseed)

    @property
    def data(self):
        return self._data

    def load_data(self):
        if os.path.exists(self._local(Production._file_res)):
            with open(self._local(Production._file_res), 'r') as f:
                self._data = json.load(f)
        # elif os.path.exists(self._local(Production._file_tmp)):
        #     with open(self._local(Production._file_tmp), 'r') as f:
        #         self._data = json.load(f)

    def save_data(self, move_to_res: bool = False):
        with open(self._local(Production._file_res), 'w') as f:
            json.dump(self._data, f, indent=2)

    # def save_data(self, move_to_res: bool = False):
    #     with open(self._local(Production._file_tmp), 'w') as f:
    #         json.dump(self._data, f, indent=2)
    #     if move_to_res:
    #         shutil.move(self._local(Production._file_tmp),
    #                     self._local(Production._file_res))

    def register_data(self, entry: dict):
        idir: str = Production._idir(entry['iseed'])
        if idir not in self._data:
            raise RuntimeError("Production::register_data: not in registry!")
        if self._data[idir][1] is not None:
            raise RuntimeError("Production::register_data: override?!")
        self._data[idir][1] = dict(elapsed_time=entry['elapsed_time'],
                                   integral=entry['integral'],
                                   error=entry['error'],
                                   chi2dof=entry['chi2dof'])
        self.save_data()

    def _append_production(self) -> dict:
        idir: str = Production._idir(self.iseed)
        if idir in self._data:
            return self._data[idir][0]

        ncores: int = self.config['production']['ncores']
        ncall: int = self.config['production']['ncall_start']
        niter: int = self.config['production']['niter']

        with self.input().open('r') as warmup_file:
            warmup = json.load(warmup_file)

        #> determine `ncall` from previous runs
        if len(self._data) > 0:
            #> this is the weighted average of "time/evt" per job
            nprod: int = 0
            ntot: int = 0
            sumt: float = 0.
            sumt2: float = 0.
            print("data = {}".format(self.data))
            for _, prod in filter(lambda item: item[1][1] is not None,
                                  self._data.items()):
                nprod += 1
                prod_ntot = prod[0]['ncall'] * prod[0]['niter']
                ntot += prod_ntot
                sumt += prod[1]['elapsed_time']
                sumt2 += prod[1]['elapsed_time']**2 / float(prod_ntot)
            avg_time_per_evt = sumt / float(ntot)
            err_time_per_evt: float = 0.
            if nprod > 1:
                err_time_per_evt = math.sqrt(sumt2 - sumt**2 /
                                             float(ntot)) / float(ntot)
            #> exactly hit min_runtime
            ncall = int(self.config['job']['min_runtime'] / avg_time_per_evt /
                        float(niter))
            #> aim for max_rumtime with a 5-sigma buffer
            #> (always overrides min_runtime)
            if err_time_per_evt > 0.:
                ncall = int(self.config['job']['max_runtime'] /
                            (avg_time_per_evt + 20 * err_time_per_evt) /
                            float(niter))

        #> None in the 2nd entry indicates that job is "pending"
        #> `policy`, `config` & `channel` are added later
        exe_args = dict(local_path=[*self.local_path,
                                    self._idir(self.iseed)],
                        input_local_path=warmup[-1][0]['local_path'],
                        exe_mode=ExecutionMode.PRODUCTION.value,
                        ncores=ncores,
                        ncall=ncall,
                        niter=niter,
                        iseed=self.iseed)
        self._data[self._idir(self.iseed)] = [exe_args, None]
        self.save_data()
        return exe_args

    def run(self):
        self.load_data()

        exe_args = self._append_production()

        if not self.complete():
            prod_exe = yield Executor.factory(
                policy=self.config['exe']['policy'],
                config=self.config,
                channel=self.channel,
                **exe_args)
            with prod_exe.open('r') as exe_file:
                self.register_data(json.load(exe_file))

        self.save_data()
