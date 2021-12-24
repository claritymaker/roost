from typing import Dict
from queue import Queue
from collections import defaultdict
from threading import RLock

import pandas as pd


class Storage:
    def __init__(self):
        self.pyobj = PyObjSlicer(self)
        # self.arrow = ArrowSlicer(self)
        self.pandas = PandasSlicer(self)
        self._staged = {"_internals": {"index": 0}}
        self.lock = RLock()
        self._committed = {}

    def stage(self, key: str, packet: Dict[str, object]):
        with self.lock:
            self._staged[key] = packet

    def commit(self):
        with self.lock:
            current_index = self._staged["_internals"]["index"]
            missing = set(sefl._committed.keys())
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


class PyObjSlicer:
    def __init__(self, parent: Storage):
        self.parent = parent

    def __getitem__(self, item):
        if isinstance(item, slice):
            with self.parent.lock:
                output = {}
                for k, v in self.parent._committed:
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

    def __getitem__(self, item):
        input = self.parent.pyobj[item]
        output = pd.DataFrame(input)
        return output

class ArrowSlicer:
    def __init__(self, parent: Storage):
        self.parent = parent
