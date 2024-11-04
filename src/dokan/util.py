"""utility module for the dokan workflow

refactor common functions and patterns here
"""

import re

from datetime import timedelta


def validate_schema(struct, schema, convert_to_type: bool = True) -> bool:
    """validate a structure against a predifined schema

    used to define & validate the structure & types in configuration files and
    data structures in the workflow.

    Parameters
    ----------
    struct :
        the datastructure to validate (mutable for conversion)
    schema :
        a datastructure that defines the expected structure and types
    convert_to_type : bool, optional
        flag to convert types in case they don't match (the default is True)
        e.g. used when reading from a JSON file where keys are converted to
        str and IntEnums are stored as int.

    Returns
    -------
    bool
        if the validation (including the conversion if chosem) was successful
    """

    # > dealing with a dictionary
    if isinstance(struct, dict) and isinstance(schema, dict):
        # > first catch the case where the type of the keys is specified
        if len(schema) == 1:
            key, val = next(iter(schema.items()))
            if isinstance(key, type):
                # > try to  convert the key back to the desired type (JSON only has str keys)
                if (
                    convert_to_type
                    and key is not str
                    and all(isinstance(k, str) for k in struct.keys())
                ):
                    for k in struct.keys():
                        struct[key(k)] = struct.pop(k)
                return all(
                    isinstance(k, key) and validate_schema(v, val, convert_to_type)
                    for k, v in struct.items()
                )
        # > default case: recursively check the dictionary
        if convert_to_type:
            for key, val in schema.items():
                if key not in struct or not isinstance(val, type):
                    continue
                if not isinstance(struct[key], val):
                    struct[key] = val(struct[key])
        return all(
            k in schema and validate_schema(struct[k], schema[k], convert_to_type) for k in struct
        )

    if isinstance(struct, list) and isinstance(schema, list):
        # > single entry of type requires all elements to be of that type
        if len(schema) == 1:
            elt = schema[0]
            if convert_to_type and isinstance(elt, type):
                for i, e in enumerate(struct):
                    if not isinstance(e, elt):
                        struct[i] = elt(e)
            return all(validate_schema(e, elt, convert_to_type) for e in struct)
        # > default case: match the length and each element
        return len(struct) == len(schema) and all(
            validate_schema(st, sc, convert_to_type) for st, sc in zip(struct, schema)
        )

    # @todo: case for a tuple? -> list with fixed length & types
    # <-> actually already covered as the default case in the list.

    if isinstance(schema, type):
        # > this can't work since we can't do an in-place conversion at this level
        # if convert_to_type and not isinstance(struct, schema):
        #     struct = schema(struct)
        return isinstance(struct, schema)

    # > no match
    return False


def parse_time_interval(interval: str) -> float:
    """convert a time interval string to seconds

    specify a time interval as a string with units (s, m, h, d, w) and convert
    the result into a float in seconds. The units are optional (default: sec).
    using multiple units is possible (e.g. "1d 2h 3m 4s").

    Parameters
    ----------
    interval : str
        time interval string to parse

    Returns
    -------
    float
        time interval in seconds
    """
    UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days", "w": "weeks"}

    return timedelta(
        **{
            UNITS.get(m.group("unit").lower(), "seconds"): float(m.group("val"))
            for m in re.finditer(
                r"(?P<val>\d+(\.\d+)?)(?P<unit>[smhdw]?)", interval.replace(" ", ""), flags=re.I
            )
        }
    ).total_seconds()
