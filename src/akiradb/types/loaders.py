import struct

from datetime import datetime
from typing import Optional, Union
from psycopg import postgres
from psycopg.adapt import AdaptersMap, Loader
from psycopg.pq import Format


class StringLoader(Loader):
    format = Format.BINARY

    def load(self, data: Union[bytes, bytearray, memoryview]) -> Optional[str]:
        res = bytes(data).decode('utf-8')
        if res == '  cypher.null':
            return None
        else:
            return res


class IntLoader(Loader):
    format = Format.BINARY

    def load(self, data: Union[bytes, bytearray, memoryview]) -> int:
        return int.from_bytes(data, byteorder='big', signed=True)


class BoolLoader(Loader):
    format = Format.BINARY

    def load(self, data: Union[bytes, bytearray, memoryview]) -> bool:
        return False if data == b'\x00' else True


class FloatLoader(Loader):
    format = Format.BINARY

    def load(self, data: Union[bytes, bytearray, memoryview]) -> float:
        return struct.unpack('>d', data)[0]


class DatetimeLoader(Loader):
    format = Format.BINARY

    def load(self, data: Union[bytes, bytearray, memoryview]) -> datetime:
        return datetime.fromtimestamp(int.from_bytes(data, byteorder='big', signed=True) / 1000)


def register_loaders(adapters: AdaptersMap):
    adapters.register_loader('varchar', StringLoader)
    adapters.register_loader('integer', IntLoader)
    adapters.register_loader('bool', BoolLoader)
    adapters.register_loader('float8', FloatLoader)
    adapters.register_loader('date', DatetimeLoader)
    adapters.register_loader(postgres.INVALID_OID, StringLoader)
