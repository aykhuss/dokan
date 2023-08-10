"""The main execution of the NNLOJET workflow
"""

#> check that we run the module as a package
assert len( __package__ ) > 0, """

The '__main__' module does not seem to have been run in the context of a
runnable package ... did you forget to add the '-m' flag?

Usage: python -m dokan
"""

import luigi
import dokan


def main():
    print(dokan.CONFIG)


if __name__ == "__main__":
    main()
