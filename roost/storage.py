from typing import Dict, Iterable, Tuple, List
from typing import Protocol
from threading import Lock

import pandas as pd
import pyarrow as pa

from roost.arrow_utils import promote_schemas, create_table, cast_available


class Storage:
    def __init__(self):
        # SLICERS
        self.pyobj = PyObjSlicer(self)
        self.arrow = ArrowSlicer(self)
        self.pandas = PandasSlicer(self)

        # INTERNALS
        self._staged = {"_internals": {"index": 0}}
        self.lock = Lock()
        self._committed = {}
        self._tables = []

        # EVENTS
        self.on_stage: List[StageSink] = []
        self.on_commit: List[StorageSink] = []
        self.on_batch: List[StorageSink] = []
        self.on_save: List[StorageSink] = []

    @property
    def num_batched_rows(self):
        return sum(t.num_rows for t in self._tables)

    @property
    def num_batched_columns(self):
        return max([t.num_columns for t in self._tables])

    @property
    def num_committed_columns(self):
        if self._tables:
            return len(set(self._tables[-1].columns).union(self._committed.keys()))
        else:
            return len(self._committed.keys())

    @property
    def num_committed_rows(self):
        return self._staged["_internals"]["index"]

    def stage(self, key: str, packet: Dict[str, object]):
        with self.lock:
            self._staged[key] = packet
            for sink in self.on_stage:
                sink.save(self, key, packet)

    def stage_many(self, *packets: Iterable[Tuple[str, Dict[str, object]]]):
        with self.lock:
            for k, p in packets:
                self._staged[k] = p
                for sink in self.on_stage:
                    sink.save(self, k, p)

    def commit(self):
        with self.lock:
            current_index = self._staged["_internals"]["index"]
            missing = set(self._committed.keys())
            for packet in self._staged.values():
                for k, v in packet.items():
                    try:
                        self._committed[k].append(v)
                    except KeyError:
                        self._committed[k] = [None] * (
                            current_index - self.num_batched_rows
                        )
                        self._committed[k].append(v)
                    missing.discard(k)

            for k in missing:
                self._committed[k].append(None)

            self._staged = {"_internals": {"index": current_index + 1}}

            for sink in self.on_commit:
                sink.save(self)

    def save(self):
        for sink in self.on_save:
            sink.save(self)

    def batch(self):
        with self.lock:
            if not self._committed:
                return
            tbl = create_table(self._committed)
            self._tables.append(tbl)
            self._committed.clear()

            # ensure all batches have consistent schema. If they don't then promote them all (expensive)
            if len(self._tables) != 1:
                promoted_schema = promote_schemas(t.schema for t in self._tables)
                self._tables = [
                    cast_available(t, promoted_schema) for t in self._tables
                ]

            for sink in self.on_batch:
                sink.save(self)


class PyObjSlicer:
    def __init__(self, parent: Storage):
        self.parent = parent

    def __getitem__(self, item) -> Dict[str, object]:
        return self.parent.arrow[item].to_pydict()


class PandasSlicer:
    def __init__(self, parent: Storage):
        self.parent = parent

    def __getitem__(self, item) -> pd.DataFrame:
        input = self.parent.pyobj[item]
        output = pd.DataFrame(input).set_index("index")
        return output


class ArrowSlicer:
    def __init__(self, parent: Storage):
        self.parent = parent

    def __getitem__(self, item) -> pa.Table:
        if isinstance(item, slice):
            row_slicer = item
            if isinstance(row_slicer, slice):
                r_start, r_stop, r_stride = row_slicer.indices(
                    self.parent.num_committed_rows
                )
                row_slicer = list(range(r_start, r_stop, r_stride))

            with self.parent.lock:
                tbls = [] + self.parent._tables + [create_table(self.parent._committed)]
                promoted_schema = promote_schemas(t.schema for t in tbls)
                tbls = [cast_available(t, promoted_schema) for t in tbls]
                tbl = pa.concat_tables(tbls, promote=True).take(row_slicer)
                return tbl

        elif isinstance(item, tuple) and len(item) == 2:
            row_slicer = item[0]
            if isinstance(row_slicer, slice):
                r_start, r_stop, r_stride = row_slicer.indices(
                    self.parent.num_committed_rows
                )
                row_slicer = list(range(r_start, r_stop, r_stride))

            col_slicer = item[1]
            if isinstance(col_slicer, str):
                col_slicer = (col_slicer,)
            if isinstance(col_slicer, slice):
                c_start, c_stop, c_stride = col_slicer.indices(
                    self.parent.num_committed_columns
                )
                col_slicer = list(range(c_start, c_stop, c_stride))

            with self.parent.lock:
                tbls = [] + self.parent._tables + [create_table(self.parent._committed)]
                promoted_schema = promote_schemas(t.schema for t in tbls)
                tbls = [cast_available(t, promoted_schema) for t in tbls]
                tbl = (
                    pa.concat_tables(tbls, promote=True)
                    .select(col_slicer)
                    .take(row_slicer)
                )
                return tbl
        else:
            raise TypeError(
                "Slicing must be either [row] or [row, column] or [row, [columns]]."
            )


class StorageSink(Protocol):
    def save(self, storage: Storage):
        ...


class StageSink(Protocol):
    def save(self, storage: Storage, key: str, packet: Dict[str, object]):
        ...
