from typing import Dict

from roost.storage import Storage


class CSVSink:
    def __init__(self, filename):
        self.filename = filename
        self.current_index = 0
        self.current_schema = None

    def save(self, storage: Storage):
        to_append = storage.pandas[self.current_index + 1 :]
        if self.current_index == 0:
            to_append.to_csv(self.filename)
        else:
            to_append.to_csv(self.filename, mode="a", header=False)

        self.current_index = to_append.index[-1]


class CSVStageSink:
    def __init__(self, filename):
        self.filename = filename
        self.current_index = 0

    def save(self, storage: Storage, key: str, packet: Dict[str, object]):
        raise NotImplementedError("TODO")
