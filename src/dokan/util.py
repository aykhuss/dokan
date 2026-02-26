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
                if convert_to_type and key is not str and all(isinstance(k, str) for k in struct):
                    struct_keys: list = list(struct.keys())
                    converted_keys: dict = {}
                    for k in struct_keys:
                        try:
                            new_key = key(k)
                        except Exception:
                            return False
                        # > detect collisions, e.g. "1" and "01" -> int(1)
                        if new_key in converted_keys and converted_keys[new_key] != k:
                            return False
                        converted_keys[new_key] = k
                    for old_key in struct_keys:
                        new_key = key(old_key)
                        struct[new_key] = struct.pop(old_key)
                return all(
                    isinstance(k, key) and validate_schema(v, val, convert_to_type) for k, v in struct.items()
                )
        # > default case: recursively check the dictionary
        if convert_to_type:
            for key, val in schema.items():
                if key not in struct or not isinstance(val, type):
                    continue
                if not isinstance(struct[key], val):
                    try:
                        struct[key] = val(struct[key])
                    except Exception:
                        return False
        return all(k in schema and validate_schema(struct[k], schema[k], convert_to_type) for k in struct)

    if isinstance(struct, list) and isinstance(schema, list):
        # > single entry of type requires all elements to be of that type
        if len(schema) == 1:
            elt = schema[0]
            if convert_to_type and isinstance(elt, type):
                for i, e in enumerate(struct):
                    if not isinstance(e, elt):
                        try:
                            struct[i] = elt(e)
                        except Exception:
                            return False
            return all(validate_schema(e, elt, convert_to_type) for e in struct)
        # > default case: match the length and each element
        return len(struct) == len(schema) and all(
            validate_schema(st, sc, convert_to_type) for st, sc in zip(struct, schema, strict=True)
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


def fill_missing(data, defaults) -> None:
    """fill in missing keys in `data` by copying entries from `defaults`

    Parameters
    ----------
    data :
        datastructure with potentialy missing entries
    defaults :
        default values to populate `data` with
    """

    # > only deal with nested-dict parts for now
    if isinstance(data, dict) and isinstance(defaults, dict):
        for key, val in defaults.items():
            if key not in data:
                data[key] = val
            else:
                fill_missing(data[key], val)


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


def format_time_interval(seconds: float) -> str:
    """convert a time interval in seconds to a human-readable string.

    Parameters
    ----------
    seconds : float
        time interval in seconds

    Returns
    -------
    str
        time interval as a human-readable string
    """
    intervals = [
        ("d", 86400),  # 1 day = 86400 seconds
        ("h", 3600),  # 1 hour = 3600 seconds
        ("m", 60),  # 1 minute = 60 seconds
        ("s", 1),
    ]
    result = []
    for unit, count in intervals:
        value = seconds // count
        if value > 0.0:
            seconds %= count
            result.append(f"{int(value)}{unit}")
    return " ".join(result) if result else "0 seconds"


patience: list[str] = [
    "He that can have patience can have what he will.",
    "Patience is the companion of wisdom.",
    "The two most powerful warriors are patience and time.",
    "Acquire a firm will and the utmost patience.",
    "One moment of patience may ward off great disaster. One moment of impatience may ruin a whole life.",
    "Patience is bitter, but its fruit is sweet.",
    "Patience is a key element of success.",
    "To lose patience is to lose the battle.",
    "Patience and fortitude conquer all things.",
    "Two things define you: your patience when you have nothing and your attitude when you have everything.",
    "Learning patience can be a difficult experience, but once conquered, you will find life is easier.",
    "Patience attracts happiness; it brings near that which is far.",
    "Patience is the best remedy for every trouble.",
    "A man who masters patience masters everything else.",
    "Trees that are slow to grow bear the best fruit.",
    "Patience is not the ability to wait, but the ability to keep a good attitude while waiting.",
    "Patience and perseverance have a magical effect before which difficulties disappear and obstacles vanish.",
    "Patience is the art of hoping.",
    "The secret of patience is to do something else in the meantime.",
    "Our patience will achieve more than our force.",
    "One minute of patience, ten years of peace.",
    "We could never learn to be brave and patient, if there were only joy in the world.",
]
