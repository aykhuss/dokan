# SPDX-FileCopyrightText: © 2024-present NNLOJET
#
# SPDX-License-Identifier: MIT

import string
import os
import shutil
import random
import time
import json
import math

import luigi
from . import Task, Executor, ExecutionMode


class DispatchChannel(Task):

    _file_res: str = "summary.json"
    _file_tmp: str = "summary.tmp"

    append: bool = luigi.BoolParameter(significant=False)

    channel: str = luigi.Parameter()
    ntot: int = luigi.IntParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print("DispatchChannel: {}".format(id(self)))

        if self.channel not in self.config["process"]["channels"]:
            raise RuntimeError("DispatchChannel: unknown channel: {}".format(
                self.channel))

        if (self.append
                and os.path.exists(self._local(DispatchChannel._file_res))):
            shutil.move(self._local(DispatchChannel._file_res),
                        self._local(DispatchChannel._file_tmp))

        #> list of tuples: (dict(exe_args), [None|dict(result)])
        self._data: list = list()
        self.load_data()

    @property
    def data(self):
        return self._data

    def load_data(self):
        if os.path.exists(self._local(DispatchChannel._file_res)):
            with open(self._local(DispatchChannel._file_res), 'r') as f:
                self._data = json.load(f)
        elif os.path.exists(self._local(DispatchChannel._file_tmp)):
            with open(self._local(DispatchChannel._file_tmp), 'r') as f:
                self._data = json.load(f)

    def save_data(self, move_to_res: bool = False):
        with open(self._local(DispatchChannel._file_tmp), 'w') as f:
            json.dump(self._data, f, indent=2)
        if move_to_res:
            shutil.move(self._local(DispatchChannel._file_tmp),
                        self._local(DispatchChannel._file_res))

    def register_data(self, entry: dict):
        #> reversed order for efficiency since new jobs are *appended*
        for ientry in reversed(range(len(self._data))):
            if self._data[ientry][0]['iseed'] != entry['iseed']:
                continue
            self._data[ientry][1] = dict(elapsed_time=entry['elapsed_time'],
                                         integral=entry['integral'],
                                         error=entry['error'],
                                         chi2dof=entry['chi2dof'])
            break
        self.save_data()

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget(self._local(DispatchChannel._file_res))

    def _warmup_done(self) -> bool:
        warmups = list(
            filter(lambda item: item[0]['exe_mode'] == ExecutionMode.WARMUP,
                   self._data))
        if len(warmups) == 0:
            return False
        #> reached max number of increments...
        if len(warmups) >= self.config['warmup']['max_increment']:
            return True
        #> next run would exceed max runtime
        if int(warmups[-1][1]['elapsed_time'] *
               self.config['warmup']['fac_increment']
               ) > self.config['job']['max_runtime']:
            return True
        #> successful convergence?
        target_rel_accuracy = 0.01
        test_accuracy = (abs(warmups[-1][1]['error'] /
                             warmups[-1][1]['integral']) < target_rel_accuracy)
        # test_chi2dof = (0.8 < warmups[-1][1]['chi2dof']
        #                 and warmups[-1][1]['chi2dof'] < 1.2)
        test_chi2dof = (warmups[-1][1]['chi2dof'] < 1.5)
        print("_warmup_done: test_accuracy = {}".format(test_accuracy))
        print("_warmup_done: test_chi2dof = {}".format(test_chi2dof))
        if test_accuracy and test_chi2dof:
            return True
        if len(warmups) < 2:
            test_scaling = False  # not enough data to test
        else:
            #> check last two iterations have correct scaling modulo buffer
            val_scaling = (
                (warmups[-1][1]['error'] / warmups[-2][1]['error'])**2 /
                self.config['warmup']['fac_increment'])
            test_scaling = (0.6 < val_scaling and val_scaling < 1.4)
        print("_warmup_done: test_scaling = {}".format(test_scaling))
        #grid_file = self._result['results'][-1]['grid_file']
        test_grid = True
        return test_chi2dof and test_scaling and test_grid

    def _append_warmup(self):
        input_local_path = []
        ncores: int = self.config['warmup']['ncores']
        ncall: int = self.config['warmup']['ncall_start']
        niter: int = self.config['warmup']['niter']
        iseed: int = self.config['job']['iseed_start']

        last_warmup = None
        for ientry in reversed(range(len(self._data))):
            if self._data[ientry][0]['exe_mode'] == ExecutionMode.WARMUP:
                last_warmup = self._data[ientry]
                break
        if last_warmup is not None:
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
        #> this generator only yields one item
        yield exe_args

    def _append_production(self):
        ncores: int = self.config['production']['ncores']
        ncall: int = self.config['production']['ncall_start']
        niter: int = self.config['production']['niter']
        iseed: int = self.config['job']['iseed_start']

        last_warmup = None
        for ientry in reversed(range(len(self._data))):
            if self._data[ientry][0]['exe_mode'] == ExecutionMode.WARMUP:
                last_warmup = self._data[ientry]
                break
        if last_warmup is None:
            raise RuntimeError("_append_production: no warmup?!")
        if last_warmup[1] is None:
            raise RuntimeError("_append_production: incomplete warmup?!")

        last_production = None
        for ientry in reversed(range(len(self._data))):
            if self._data[ientry][0]['exe_mode'] == ExecutionMode.PRODUCTION:
                last_production = self._data[ientry]
                break
        if last_production is not None:
            iseed = max(iseed, last_production[0]['iseed'] + 1)
            #> time to determine `ncall` from previous runs
            #> this is the weighted average of "time/evt" per job
            nprod: int = 0
            ntot: int = 0
            sumt: float = 0.
            sumt2: float = 0.
            for prod in filter(
                    lambda item: item[0]['exe_mode'] == ExecutionMode.
                    PRODUCTION and item[1] is not None, self._data):
                nprod += 1
                prod_ntot = prod[0]['ncall'] * prod[0]['nit']
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
                            (avg_time_per_evt + 5 * err_time_per_evt) /
                            float(niter))

        #> now everything set up for generating seeds
        ntot_remaining: int = self.ntot
        while ntot_remaining > (ncall * niter) / 2.:
            #> None in the 2nd entry indicates that job is "pending"
            #> `policy`, `config` & `channel` are added later
            exe_args = dict(local_path=[*self.local_path, "p{}".format(iseed)],
                            input_local_path=last_warmup[0]['local_path'],
                            exe_mode=ExecutionMode.PRODUCTION.value,
                            ncores=ncores,
                            ncall=ncall,
                            niter=niter,
                            iseed=iseed)
            self._data.append([exe_args, None])
            #> prepare for next iteration before `yield`
            iseed += 1
            ntot_remaining -= ncall * niter
            yield exe_args

    def run(self):
        self.load_data()

        #> collect all previous results & register
        prev_exe = yield [
            Executor.factory(policy=self.config['exe']['policy'],
                             config=self.config,
                             channel=self.channel,
                             **job_entry[0])
            for job_entry in filter(lambda item: item[1] is None, self._data)
        ]
        for exe in prev_exe:
            with exe.open('r') as exe_file:
                self.register_data(json.load(exe_file))

        #> warmup jobs if incomplete
        if not self._warmup_done():
            warmups = [*self._append_warmup()]
            #> must save before yield so next instance can load correct data!
            self.save_data()
            yield [
                Executor.factory(policy=self.config['exe']['policy'],
                                 config=self.config,
                                 channel=self.channel,
                                 **exe_args) for exe_args in warmups
            ]

        #> if the last batch was a production, we're done
        #> NOPE: append would be broken this way :(
        if self._data[-1][0]['exe_mode'] == ExecutionMode.PRODUCTION:
            self.save_data(move_to_res=True)
            return

        #> time for production jobs
        productions = [*self._append_production()]
        #> must save before yield so next instance can load correct data!
        self.save_data()
        # yield [
        #     Executor.factory(policy=self.config['exe']['policy'],
        #                      config=self.config,
        #                      channel=self.channel,
        #                      **exe_args) for exe_args in productions
        # ]

        self.save_data(move_to_res=True)
