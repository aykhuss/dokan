# dokan (土管)

[![PyPI - Version](https://img.shields.io/pypi/v/dokan.svg)](https://pypi.org/project/dokan)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dokan.svg)](https://pypi.org/project/dokan)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

> <img src="https://raw.githubusercontent.com/aykhuss/dokan/main/doc/img/dokan.png" height="23px">&emsp;A pipeline for automating the NNLOJET workflow

-----

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
  - [Initialization](#1-initialization)
  - [Configuration](#2-configuration)
  - [Submission](#3-submission)
  - [Monitoring & Recovery](#4-monitoring--recovery)
- [Shell Completion](#shell-completion)
- [License](#license)

**dokan** implements an automated workflow for [NNLOJET](https://nnlojet.hepforge.org/) computations based on the [luigi](https://github.com/spotify/luigi) framework. It handles job submission, monitoring, result merging, and error recovery.

## Prerequisites

*   **Python:** >= 3.10
*   **NNLOJET:** A working installation of the NNLOJET executable is required to run calculations.

## Installation

### Release version

Install easily using `pip` or `uv`:

```shell
# using pip
pip install dokan

# using uv (recommended)
uv tool install dokan
```

### Development version

To install for development, clone the repository:

```shell
git clone https://github.com/aykhuss/dokan.git
cd dokan
```

You can set up your environment using `uv` (recommended) or `pip`:

#### Using `uv`
Sync the environment and install the tool in editable mode:
```shell
uv sync
uv tool install -e .
```

#### Using `pip`
```shell
pip install -e .
```

## Usage

The main command is `nnlojet-run`. You can always use `--help` to see available options.

### 1. Initialization
Initialize a new run directory from a runcard.

```shell
# Create a new folder named after the `RUN` name in the runcard
nnlojet-run init example.run

# Or specify a custom output directory
nnlojet-run init example.run -o my_run_dir
```

### 2. Configuration
(Optional) Re-configure default settings for the calculation.

```shell
nnlojet-run config my_run_dir
```

### 3. Submission
Submit the run to your execution backend (Local, Slurm, HTCondor).

```shell
# Submit with defaults
nnlojet-run submit my_run_dir

# Override defaults (e.g., runtime, number of jobs, accuracy)
nnlojet-run submit my_run_dir \
    --job-max-runtime 1h30m \
    --jobs-max-total 10 \
    --target-rel-acc 1e-2
```

### 4. Monitoring & Recovery
Check the health of your run and recover from failures.

```shell
# Check status
nnlojet-run doctor my_run_dir

# Recover jobs that may have crashed but produced output
nnlojet-run doctor my_run_dir --recover
```

### 5. Finalization
Merge all results into final grids and tables.

```shell
nnlojet-run finalize my_run_dir
```

## Shell Completion

Both `nnlojet-run` and `nnlojet-merge` can generate their own shell completion
scripts (for `bash`, `zsh`, and `tcsh`) directly from their argument parsers via
the `--print-completion` flag, so the completions never go out of sync with the
CLI.

The quickest way is to source the generated script in your shell startup file.
For **bash** (`~/.bashrc`):

```shell
source <(nnlojet-run --print-completion bash)
source <(nnlojet-merge --print-completion bash)
```

For **zsh** (`~/.zshrc`), replace `bash` with `zsh`; for **tcsh** (`~/.tcshrc`),
use `tcsh` and `eval` the output:

```shell
eval "`nnlojet-run --print-completion tcsh`"
```

The `source <(...)` form regenerates the script on every new shell. If you'd
rather avoid that small startup cost, write the script to a file once and source
that instead, regenerating only after upgrading dokan:

```shell
nnlojet-run --print-completion bash > ~/.local/share/bash-completion/completions/nnlojet-run
nnlojet-merge --print-completion bash > ~/.local/share/bash-completion/completions/nnlojet-merge
```

## License

`dokan` is distributed under the terms of the [GPL-3.0](https://spdx.org/licenses/GPL-3.0-or-later.html) license.

