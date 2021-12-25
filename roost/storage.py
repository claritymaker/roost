from typing import Dict, Iterable, Tuple, List
from typing import Protocol
from threading import Lock

import pandas as pd
import pyarrow as pa


class Storage:
    def __init__(self):
        # SLICERS
        self.pyobj = PyObjSlicer(self)
        # self.arrow = ArrowSlicer(self)
        self.pandas = PandasSlicer(self)

        # INTERNALS
        self._staged = {"_internals": {"index": 0}}
        self.lock = Lock()
        self._committed = {}
        self._batches = []
        self._batched_schema = None

        # EVENTS
        self.on_stage: List[StageSink] = []
        self.on_commit: List[StorageSink] = []
        self.on_save: List[StorageSink] = []

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
                        self._committed[k] = [None] * current_index
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
            batch = pa.RecordBatch.from_pydict(self._committed)
            self._batches.append(batch)
            self._committed.clear()
            # if not self._batched_schema:
            #     self._batched_schema =


class PyObjSlicer:
    def __init__(self, parent: Storage):
        self.parent = parent

    def __getitem__(self, item) -> Dict[str, object]:
        if isinstance(item, slice):
            with self.parent.lock:
                output = {}
                for k, v in self.parent._committed.items():
                    output[k] = v[item]
        elif isinstance(item, tuple) and len(item) == 2:
            row_slicer = item[0]
            col_slicer = item[1]
            if isinstance(col_slicer, str):
                col_slicer = (col_slicer, )

            with self.parent.lock:
                output = {}
                for k, v in self.parent._committed:
                    if k not in col_slicer:
                        continue
                    output[k] = v[row_slicer]
        else:
            raise TypeError("Slicing must be either [row] or [row, column] or [row, [columns]].")
        return output


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


class StorageSink(Protocol):
    def save(self, storage: Storage):
        ...

class StageSink(Protocol):
    def save(self, storage: Storage, key: str, packet: Dict[str, object]):
        ...
