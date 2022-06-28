from typing import Any
from collections import defaultdict
from dataclasses import dataclass, field


import pyarrow as pa

from roost.arrow_utils import create_table


@dataclass
class ProtoRecordBatch:
    starting_index: int = field(hash=False)
    values: dict[str, list] = field(default_factory=lambda: defaultdict(list))
    schema: dict[str, type] | None = None
    len: int = field(hash=False, compare=False, init=False, default=0)

    def add(self, packet: dict[str, Any]):
        for new_key in set(packet.keys() - self.values.keys()):
            self.values[new_key].extend((None, ) * self.len)

        for k, v in packet.items():
            # TODO: enforce schema
            self.values[k].append(v)

        for missing_key in set(self.values.keys() - packet.keys()):
            self.values[missing_key].append(None)

        self.len += 1

    def to_table(self) -> pa.Table:
        # TODO: enforce schema
        return create_table(self.values)

    def slice(self, *_, **__):
        # TODO: add slicing
        raise NotImplementedError("Slicing not implemented yet")

    def rotate(self):
        self.starting_index += self.len
        self.len = 0
        self.values.clear()
