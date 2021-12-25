from random import randint
from roost.storage import Storage
from datetime import datetime
from enum import Enum, auto
from typing import NamedTuple
import pandas as pd
from collections import defaultdict

class DummyEnum(Enum):
    en_a = auto()
    en_b = auto()
    en_c = auto()


class DummyNamedTuple(NamedTuple):
    dnt_a: int
    dnt_b: int


def generate_data(idx: int, key_prefix=""):
    return {
        f"{key_prefix}int": idx,
        f"{key_prefix}float": idx + 0.1,
        f"{key_prefix}str": str(idx),
        f"{key_prefix}str_varwidth": str(idx)*randint(0, 10),
        f"{key_prefix}datetime": datetime.now(),
        f"{key_prefix}enum": DummyEnum(1 + (idx % 3)),
        f"{key_prefix}named_tuple": DummyNamedTuple(dnt_a=idx, dnt_b=-idx),
        f"{key_prefix}list": list(range(idx, idx+10)),
        f"{key_prefix}dict": {"dict_a": idx, "dict_b": list(range(idx, idx+10))},
        f"{key_prefix}df": pd.DataFrame({"dict_a": list(range(idx, idx+1)), "dict_b": list(range(idx, idx-1, -1))}),
    }


def test_routes():
    s = Storage()
    n_rows = 10
    k1 = defaultdict(list)
    k2 = defaultdict(list)
    for idx in range(n_rows):
        tmp1 = generate_data(idx, "k1_")
        for k, v in tmp1.items():
            k1[k].append(v)
        tmp2 = generate_data(idx, "k2_")
        for k, v in tmp2.items():
            k2[k].append(v)

        s.stage("key1", tmp1)
        s.stage("key2", tmp2)
        s.commit()

    desired_output = {**k1, **k2}
    # desired_output = {k: [v] for k, v in desired_output.items()}
    desired_output["index"] = list(range(n_rows))
    output = s.pyobj[:]
    assert output == desired_output
