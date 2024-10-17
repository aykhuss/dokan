def validate_schema(struct, schema, convert_to_type: bool = True) -> bool:
    """[summary]

    [description]

    Parameters
    ----------
    struct : [type]
        [description]
    schema : [type]
        [description]
    convert_to_type : bool, optional
        [description] (the default is True, which [default_description])

    Returns
    -------
    bool
        [description]
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
            k in schema and validate_schema(struct[k], schema[k], convert_to_type)
            for k in struct
        )

    if isinstance(struct, list) and isinstance(schema, list):
        # > single entry of type requires all elements to be of that type
        if len(schema) == 1 and isinstance((elt := schema[0]), type):
            if convert_to_type:
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
        # > this can't work since we can't to an in-place conversion at this level
        # if convert_to_type and not isinstance(struct, schema):
        #     struct = schema(struct)
        return isinstance(struct, schema)

    # > no match
    return False
