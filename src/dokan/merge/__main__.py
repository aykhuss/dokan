"""`nnlojet-merge` command-line entrypoint.

A standalone, database-free merge driver that mimics `nnlojet-combine.py`:
reads a `combine.ini`, builds a luigi DAG over the dokan merge core and runs it
with the requested number of parallel workers.
"""

import argparse
import multiprocessing as mp
import sys

import luigi

from ._combine import Combine, build_config


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="nnlojet-merge",
        description="Merge NNLOJET histogram files using the dokan merge core.",
    )
    parser.add_argument(
        "-C", "--config", default="combine.ini", help="combine configuration file (default: combine.ini)"
    )
    parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        nargs="?",
        const=mp.cpu_count(),
        default=1,
        help="number of parallel workers (no value: all CPUs)",
    )
    args = parser.parse_args()

    config = build_config(args.config)
    success = luigi.build(
        [Combine(config=config)],
        workers=args.jobs,
        local_scheduler=True,
        log_level="WARNING",
    )
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
