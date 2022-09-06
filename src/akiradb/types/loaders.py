from psycopg.abc import Buffer
from psycopg.adapt import Loader


class IntBinaryLoader(Loader):
    def load(self, data: Buffer) -> int:
        return int.from_bytes(data, byteorder='big', signed=True)
