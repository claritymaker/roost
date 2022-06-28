from threading import Lock
from typing import Dict
from collections import defaultdict

import pyarrow as pa

from roost.arrow_utils import cast_available, promote_schemas
from roost.proto_record_batch import ProtoRecordBatch


class Storage:
    def __init__(self, schemas: dict[str, dict[str, type] | None] | None = None):
        self._rules = []
        self._unbatched: dict[str, ProtoRecordBatch] = {}
        self._tables = {}
        self._counts = defaultdict(lambda: -1)

        self.schemas = schemas or {}

        # INTERNALS
        self.lock = Lock()

    def add(self, key: str, packet: Dict[str, object]):
        with self.lock:
            self._counts[key] += 1
            if key not in self._unbatched:
                prb = ProtoRecordBatch(self._counts[key], schema=self.schemas.get(key))
                self._unbatched[key] = prb
            self._unbatched[key].add(packet)
            # TODO: triggers on add

    def batch(self, key):
        with self.lock:
            # exit early
            if self._unbatched[key].len == 0:
                return

            tbl = self._unbatched[key].to_table()
            self._unbatched[key].rotate()

            if key not in self._tables:
                self._tables[key] = tbl
            else:
                # ensure all batches have consistent schema. If they don't then promote them all (expensive)
                promoted_schema = promote_schemas([self._tables[key], tbl])
                self._tables[key] = pa.concat_tables([
                    cast_available(self._tables[key], promoted_schema),
                    cast_available(tbl, promoted_schema),
                ])

            # TODO: triggers on batch

    def add_rule(self, rule):
        # TODO: add rules
        pass

    def trigger_rule(self, rule):
        # TODO: add rules
        pass

    def finalize(self):
        # TODO: add rules
        pass

# Cases
# Taking snapshot
# Dropping columns from packet
# mapping packet -> adding column
# Creating derived packet
# Triggered packet
# Saving on add
# Saving on batch
# saving on finalize

