from typing import Iterable
import pyarrow as pa
from functools import lru_cache


@lru_cache()
def promote_types(types: Iterable[pa.DataType]):
    known_types = {
        pa.bool_(): [pa.bool_(), pa.int8(), pa.int16(), pa.int32(), pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64(), pa.float16(), pa.float32(), pa.float64()],
        pa.uint8(): [pa.int16(), pa.int32(), pa.int64(), pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64(), pa.float16(), pa.float32(), pa.float64()],
        pa.uint16(): [pa.int32(), pa.int64(), pa.uint16(), pa.uint32(), pa.uint64(), pa.float16(), pa.float32(), pa.float64()],
        pa.uint32(): [pa.int64(), pa.uint32(), pa.uint64(), pa.float32(), pa.float64()],
        pa.uint64(): [pa.int64(), pa.uint64(), pa.float16(), pa.float64()],
        pa.int8(): [pa.int8(), pa.int16(), pa.int32(), pa.int64(), pa.float16(), pa.float32(), pa.float64()],
        pa.int16(): [pa.int16(), pa.int32(), pa.int64(), pa.float16(), pa.float32(), pa.float64()],
        pa.int32(): [pa.int32(), pa.int64(), pa.float16(), pa.float32(), pa.float64()],
        pa.int64(): [pa.int32(), pa.int64(), pa.float16(), pa.float32(), pa.float64()],
        pa.float16(): [pa.float16(), pa.float32(), pa.float64()],
        pa.float32(): [pa.float32(), pa.float64()],
        pa.float64(): [pa.float64()],
    }

    f_types = set(types)
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


if __name__ == "__main__":
    t1 = pa.Table.from_pydict({"a": [1, 2]})
    t2 = pa.Table.from_pydict({"a": [1.1, 2]})
    t3 = pa.Table.from_pydict({"b": [1, 2]})
    promoted_schema = promote_schemas([t1.schema, t2.schema, t3.schema])
