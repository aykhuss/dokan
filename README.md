# dokan (土管)

[![PyPI - Version](https://img.shields.io/pypi/v/dokan.svg)](https://pypi.org/project/dokan)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dokan.svg)](https://pypi.org/project/dokan)

> <img src="./doc/img/dokan.png" height="23px">&emsp;A pipeline for automating the NNLOJET workflow

-----


## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [License](#license)

This is the implementation of an automated workflow for NNLOJET computations based on the [luigi](https://github.com/spotify/luigi) framework. 


## Installation

### Release version

todo: publish on pypi

### Development version

To install the development version, first clone this repository
```shell
git clone git@github.com:aykhuss/dokan.git
cd dokan
```
You can either install the tool using `pip` or `uv`.

#### `pip`
Within the repository directory, run (add `--force-reinstall` if necessary)
```shell
pip install -e .
```
This should install the `nnlojet-run` command for you.

#### `uv`
[`uv`](https://docs.astral.sh/uv/) is a modern and more powerful alternative to `pip`.
`dokan` can be installed by running within the repository directory (add `--force` if necessary):
```shell
uv tool install -e .
```
With this, the `nnlojet-run` executable should be installed and available.


## Usage

Some example usage:
```shell

# general help
nnlojet-run --help

### subcommand `init` 
#   to initialise a new run
#   
# help
nnlojet-run init --help

# take any runcard and use it to initialise a new run
nnlojet-run init example.run
# this will create a new folder under the current path named after the `RUN` name in the runcard
# or you can add `-o <RUN_PATH>` to specify a location where the run should be initialised

### subcommand `config` 
#   to re-configure default settings for a calculation
#   
nnlojet-run config example_run_Z_8TeV

### subcommand `submit` 
#   to submit a run
#   
# help
nnlojet-run submit --help

# submit jobs with default configuration as set during `init`/`config`
nnlojet-run submit example_run_Z_8TeV 

# you can override defaults by passing the desired settings as options, e.g.
nnlojet-run submit example_run_Z_8TeV --job-max-runtime 1h30m --jobs-max-total 10 --target-rel-acc 1e-2

```


## License

`dokan` is distributed under the terms of the [GPL-3.0](https://spdx.org/licenses/GPL-3.0-or-later.html) license.

