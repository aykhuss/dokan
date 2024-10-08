# dokan (土管)

[![PyPI - Version](https://img.shields.io/pypi/v/dokan.svg)](https://pypi.org/project/dokan)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dokan.svg)](https://pypi.org/project/dokan)

> <img src="./doc/img/pipe.png" height="23px">&emsp;<img src="./doc/img/dokan.webp" height="23px">&emsp;A pipeline for automating the NNLOJET workflow

-----

## Table of Contents

- [Installation](#installation)
- [License](#license)
- [Usage](#usage)

This is the implementation of an automated workflow for NNLOJET computations based on the [luigi](https://github.com/spotify/luigi) framework. 

## Installation

```console
pip install -e .
```

## Usage

Some example usage:
```console

# help
pyton -m dokan --help
pyton -m dokan init --help
pyton -m dokan submit --help

# initialise a job
pyton -m dokan init example.run

# submit a job
python -m dokan submit example

```


## License

`dokan` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.

