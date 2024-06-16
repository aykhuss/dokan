import string
import os
import shutil
import random
import time
import json
import math

import luigi
from . import *


class Warmup(Task):

    _file_res: str = "warmup.json"
    _file_tmp: str = "warmup.tmp"

    channel: str = luigi.Parameter()

    @property
    def resources(self) -> dict:
        """limit to one warmup in `run` at a time
        """
        return {"warmup_" + '_'.join(map(str, self.local_path)): 1}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if self.channel not in self.config["process"]["channels"]:
            raise RuntimeError("Warmup: unknown channel: {}".format(
                self.channel))

        #> list of tuples: [ dict(exe_args), [None|dict(result)] ]
        self._data: list = list()
        self.load_data()

        #> not done yet: res -> tmp
        if (not self._done()
                and os.path.exists(self._local(Warmup._file_res))):
            shutil.move(self._local(Warmup._file_res),
                        self._local(Warmup._file_tmp))

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget(self._local(Warmup._file_res))

    @property
    def data(self):
        return self._data

    def load_data(self):
        if os.path.exists(self._local(Warmup._file_res)):
            with open(self._local(Warmup._file_res), 'r') as f:
                self._data = json.load(f)
        elif os.path.exists(self._local(Warmup._file_tmp)):
            with open(self._local(Warmup._file_tmp), 'r') as f:
                self._data = json.load(f)

    def save_data(self, move_to_res: bool = False):
        with open(self._local(Warmup._file_tmp), 'w') as f:
            json.dump(self._data, f, indent=2)
        if move_to_res:
            shutil.move(self._local(Warmup._file_tmp),
                        self._local(Warmup._file_res))

    def register_data(self, entry: dict):
        if len(self._data) == 0:
            raise RuntimeError("Warmup::register_data: no entry?!")
        if self._data[-1][1] is not None:
            raise RuntimeError("Warmup::register_data: already filled entry?!")
        if self._data[-1][0]['iseed'] != entry['iseed']:
            raise RuntimeError(
                "Warmup::register_data: seed mismatch: {} != {}".format(
                    self._data[-1][0]['iseed'], entry['iseed']))
        self._data[-1][1] = dict(elapsed_time=entry['elapsed_time'],
                                 integral=entry['integral'],
                                 error=entry['error'],
                                 chi2dof=entry['chi2dof'])
        self.save_data()

    def _done(self) -> bool:
        if len(self._data) == 0:
            return False
        #> reached max number of increments...
        if len(self._data) >= self.config['warmup']['max_increment']:
            return True
        #> next run would exceed max runtime
        if int(self._data[-1][1]['elapsed_time'] *
               self.config['warmup']['fac_increment']
               ) > self.config['job']['max_runtime']:
            return True
        #> successful convergence?
        target_rel_accuracy = 0.01
        test_accuracy = (abs(self._data[-1][1]['error'] /
                             self._data[-1][1]['integral'])
                         < target_rel_accuracy)
        # test_chi2dof = (0.8 < self._data[-1][1]['chi2dof']
        #                 and self._data[-1][1]['chi2dof'] < 1.2)
        test_chi2dof = (self._data[-1][1]['chi2dof'] < 1.5)
        print("_done: test_accuracy = {}".format(test_accuracy))
        print("_done: test_chi2dof = {}".format(test_chi2dof))
        if test_accuracy and test_chi2dof:
            return True
        if len(self._data) < 2:
            test_scaling = False  # not enough data to test
        else:
            #> check last two iterations have correct scaling modulo buffer
            val_scaling = (
                (self._data[-1][1]['error'] / self._data[-2][1]['error'])**2 /
                self.config['warmup']['fac_increment'])
            test_scaling = (0.6 < val_scaling and val_scaling < 1.4)
        print("_done: test_scaling = {}".format(test_scaling))
        #grid_file = self._result['results'][-1]['grid_file']
        test_grid = True  #@todo
        return test_chi2dof and test_scaling and test_grid

    def _append_warmup(self) -> dict:
        input_local_path = []
        ncores: int = self.config['warmup']['ncores']
        ncall: int = self.config['warmup']['ncall_start']
        niter: int = self.config['warmup']['niter']
        iseed: int = self.config['job']['iseed_start']

        last_warmup = None
        if len(self._data) > 0:
            last_warmup = self._data[-1]
            if last_warmup[1] is None:
                raise RuntimeError("_append_warmup: incomplete warmup?!")
            iseed = max(iseed, last_warmup[0]['iseed'] + 1)
            ncall = int(last_warmup[0]['ncall'] *
                        self.config['warmup']['fac_increment'])
            input_local_path = last_warmup[0]['local_path']

        #> None in the 2nd entry indicates that job is "pending"
        #> `policy`, `config` & `channel` are added later
        exe_args = dict(local_path=[*self.local_path, "w{}".format(iseed)],
                        input_local_path=input_local_path,
                        exe_mode=ExecutionMode.WARMUP.value,
                        ncores=ncores,
                        ncall=ncall,
                        niter=niter,
                        iseed=iseed)
        self._data.append([exe_args, None])
        self.save_data()
        return exe_args

    def run(self):
        self.load_data()

        #> collect previous result (if exists) & register
        if len(self._data) > 0:
            if self._data[-1][1] is None:
                prev_exe = yield Executor.factory(
                    policy=self.config['exe']['policy'],
                    config=self.config,
                    channel=self.channel,
                    **self._data[-1][0])
                with prev_exe.open('r') as exe_file:
                    self.register_data(json.load(exe_file))

        #> warmup jobs if incomplete
        if not self._done():
            yield Executor.factory(policy=self.config['exe']['policy'],
                                   config=self.config,
                                   channel=self.channel,
                                   **self._append_warmup())

        self.save_data(move_to_res=True)
