import pyarrow.feather
import pyarrow.ipc
import pyarrow.parquet

from roost.storage import Storage


class ParquetSink:
    def __init__(self, filename):
        self.filename = filename

    def save(self, storage: Storage):
        tbl = storage.arrow[:]
        pyarrow.parquet.write_table(tbl, self.filename)
