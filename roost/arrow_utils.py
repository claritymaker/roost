from typing import Iterable, Sequence, Dict, Any
from enum import Enum
from copy import deepcopy

import pandas as pd
import pyarrow as pa
from functools import lru_cache
from pyarrow import compute


@lru_cache()
def promote_types(types: Iterable[pa.DataType]):
    f_types = set(types)
    if len(f_types) == 1:
        return f_types.pop()

    known_types = {
        pa.bool_(): [pa.bool_(), pa.int8(), pa.int16(), pa.int32(), pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64(), pa.float16(), pa.float32(), pa.float64(), pa.string()],
        pa.uint8(): [pa.int16(), pa.int32(), pa.int64(), pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64(), pa.float16(), pa.float32(), pa.float64(), pa.string()],
        pa.uint16(): [pa.int32(), pa.int64(), pa.uint16(), pa.uint32(), pa.uint64(), pa.float16(), pa.float32(), pa.float64(), pa.string()],
        pa.uint32(): [pa.int64(), pa.uint32(), pa.uint64(), pa.float32(), pa.float64(), pa.string()],
        pa.uint64(): [pa.int64(), pa.uint64(), pa.float16(), pa.float64(), pa.string()],
        pa.int8(): [pa.int8(), pa.int16(), pa.int32(), pa.int64(), pa.float16(), pa.float32(), pa.float64(), pa.string()],
        pa.int16(): [pa.int16(), pa.int32(), pa.int64(), pa.float16(), pa.float32(), pa.float64(), pa.string()],
        pa.int32(): [pa.int32(), pa.int64(), pa.float16(), pa.float32(), pa.float64(), pa.string()],
        pa.int64(): [pa.int32(), pa.int64(), pa.float16(), pa.float32(), pa.float64(), pa.string()],
        pa.float16(): [pa.float16(), pa.float32(), pa.float64(), pa.string()],
        pa.float32(): [pa.float32(), pa.float64(), pa.string()],
        pa.float64(): [pa.float64(), pa.string()],
        pa.timestamp('ns'): [pa.timestamp('s'), pa.timestamp('ms'), pa.timestamp('us'), pa.string()],
        pa.timestamp('us'): [pa.timestamp('s'), pa.timestamp('ms'), pa.string()],
        pa.timestamp('ms'): [pa.timestamp('s'), pa.string()],
        pa.timestamp('s'): [pa.string()],
    }

    for base_t, base_up in deepcopy(known_types).items():
        list_types = [pa.list_(t) for t in base_up]
        known_types[base_t].extend(list_types)
        known_types[pa.list_(base_t)] = list_types

    known_types[pa.string()] = [pa.string()],

    avail_types = set(known_types[pa.bool_()])
    for f in f_types:
        avail_types = avail_types.intersection(known_types[f])

    # choice order is also order in known_types
    for t in known_types.keys():
        if t in avail_types:
            return t
    raise TypeError(f"Cannot find common type between fields")

# try:
#     union_schema =  pa.unify_schemas([first_schema, second_schema, third_schema])
# except pa.ArrowInvalid as e:
#     print(e)

def promote_schemas(schemas: Iterable[pa.Schema]):
    schemas = list(schemas)
    output_schema = schemas[0]
    all_names = set(n for s in schemas for n in s.names)
    for n in all_names:
        specific_fields = []
        for s in schemas:
            try:
                specific_fields.append(s.field(n))
            except KeyError:
                pass
        promoted_type = promote_types(f.type for f in specific_fields)

        if (idx := output_schema.get_field_index(n)) >= 0:
            output_schema = output_schema.set(idx, pa.field(n, promoted_type))
        else:
            output_schema = output_schema.append(pa.field(n, promoted_type))

    return output_schema


def create_table(pyobj: Dict[str, Sequence[Any]]) -> pa.Table:
    # This funciton exists because pa.Table.from_pyobj can't handle a column like [2, 4, "Error"]
    # so this will return ["2", "4", "Error"]
    names = []
    cols = []
    for k, v in pyobj.items():
        names.append(k)
        try:
            arr = pa.array(v)
        except pa.ArrowInvalid as e:
            # peek at type, make educated guesses based on types
            # TODO: pick first (or last?) non-None value
            if isinstance(v[0], Enum):
                # If the first value is an Enum, lets try to assume all of them are also enums
                # Maybe we should do val.name instead of str(val)
                v = [str(val) for val in v]
                arr = pa.array(pd.array(v), type=pa.dictionary(pa.int8(), pa.string()))
            if isinstance(v[0], pd.DataFrame):
                # If the first value is an Enum, lets try to assume all of them are also enums
                # Maybe we should do val.name instead of str(val)
                v = [str(val) for val in v]
                arr = pa.array(pd.array(v), type=pa.dictionary(pa.int8(), pa.string()))
            else:
                # fallback to pandas, which will handle string conversion
                arr = pa.array(pd.array(v))

        cols.append(arr)

    return pa.Table.from_arrays(arrays=cols, names=names)


def cast_available(table: pa.Table, schema: pa.Schema) -> pa.Table:
    # Assumes that table's names are a subset of schema's names
    common_names = set(schema.names).difference(table.column_names)
    for k in common_names:
        idx = schema.get_field_index(k)
        schema = schema.remove(idx)

    s2 = pa.schema((schema.field(n) for n in table.schema.names), metadata=schema.metadata)

    return table.cast(s2)
