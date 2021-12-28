import pyarrow as pa

from roost.arrow_utils import cast_available, create_table, promote_schemas


def test_basic_concat_tables():
    t1 = create_table({"a": [1, 2]})
    t2 = create_table({"a": [1.1, 2], "c": [5, "hi"]})
    t3 = create_table({"b": [1, 2]})
    promoted_schema = promote_schemas([t1.schema, t2.schema, t3.schema])
    t1 = cast_available(t1, promoted_schema)
    t2 = cast_available(t2, promoted_schema)
    t3 = cast_available(t3, promoted_schema)
    tt = pa.concat_tables([t1, t2, t3], promote=True)
