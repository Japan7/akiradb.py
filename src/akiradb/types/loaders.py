from datetime import datetime
from typing import Optional
from psycopg.abc import Buffer
from psycopg.adapt import Loader
from psycopg.pq import Format


class StringLoader(Loader):
    format = Format.BINARY

    def load(self, data: Buffer) -> Optional[str]:
        res = bytes(data).decode('utf-8')
        if res == '  cypher.null':
            return None
        else:
            return res


class DatetimeLoader(Loader):
    format = Format.BINARY

    def load(self, data: Buffer) -> datetime:
        return datetime.fromtimestamp(int.from_bytes(data, byteorder='big', signed=True) / 1000)
