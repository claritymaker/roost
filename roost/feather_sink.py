import pyarrow.feather
import pyarrow.ipc

from roost.storage import Storage


class FeatherSink:
    def __init__(self, filename):
        self.filename = filename

    def save(self, storage: Storage):
        to_append = storage.arrow[:]
        pyarrow.feather.write_feather(to_append, self.filename)
