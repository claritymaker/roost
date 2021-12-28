from typing import Optional, NamedTuple
import pyarrow.feather
import pyarrow.ipc
import pyarrow.parquet

from roost.storage import Storage


class ArrowSinkPosition(NamedTuple):
    index: int
    partition: int
    schema: Optional[pyarrow.Schema]
    writer: Optional[pyarrow.ipc.RecordBatchStreamWriter]


class ArrowSink:
    def __init__(self, filename):
        self.filename = filename
        self.position: ArrowSinkPosition = ArrowSinkPosition(0, -1, None, None)

    def save(self, storage: Storage):
        tbl = storage.arrow[self.position.index :]

        if self.position.schema != tbl.schema:
            filename = self.filename + f"_{self.position.partition + 1}"
            self.position = ArrowSinkPosition(
                tbl.num_rows + self.position.index,
                self.position.partition + 1,
                tbl.schema,
                pyarrow.ipc.new_stream(filename, tbl.schema),
            )

        self.position.writer.write_table(tbl)
