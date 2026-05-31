"""`nnlojet-merge` command-line entrypoint.

A standalone, database-free merge driver that mimics `nnlojet-combine.py`:
reads a `combine.ini`, builds a luigi DAG over the dokan merge core and runs it
with the requested number of parallel workers.
"""

import argparse
import logging
import multiprocessing as mp
import sys

import luigi

from ._combine import Combine, build_config

_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("[nnlojet-merge] %(message)s"))
_merge_log = logging.getLogger("dokan.merge")
_merge_log.setLevel(logging.INFO)
_merge_log.addHandler(_handler)
_merge_log.propagate = False


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

    _merge_log.info("reading config from %s", args.config)
    config = build_config(args.config)
    combine = config["combine"]
    n_parts = len(combine["parts"])
    n_merge = len(combine["merge"])
    n_final = len(combine["final"])
    n_obs = len(config["run"]["histograms"])
    _merge_log.info(
        "%d part(s), %d intermediate merge(s), %d final combination(s), %d observable(s)",
        n_parts,
        n_merge,
        n_final,
        n_obs,
    )
    _merge_log.info("running with %d worker(s)", args.jobs)

    success = luigi.build(
        [Combine(config=config)],
        workers=args.jobs,
        local_scheduler=True,
        log_level="WARNING",
    )
    if success:
        _merge_log.info("done")
    else:
        _merge_log.error("one or more tasks failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
