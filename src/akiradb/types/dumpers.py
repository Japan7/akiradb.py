from decimal import Decimal
from datetime import datetime
from psycopg.adapt import Dumper
from psycopg.pq import Format
from psycopg.adapt import AdaptersMap


class StringDumper(Dumper):
    format = Format.TEXT

    def dump(self, obj: str) -> bytes:
        return obj.encode('utf-8')


class IntDumper(Dumper):
    format = Format.TEXT

    def dump(self, obj: int) -> bytes:
        return repr(obj).encode('utf-8')


class BoolDumper(Dumper):
    format = Format.TEXT

    def dump(self, obj: bool) -> bytes:
        return ("true" if obj else "false").encode('utf-8')


class FloatDumper(Dumper):
    format = Format.TEXT

    def dump(self, obj: float) -> bytes:
        return format(Decimal(obj), 'f').encode('utf-8')


class DatetimeDumper(Dumper):
    format = Format.TEXT

    def dump(self, obj: datetime) -> bytes:
        return repr(int(obj.timestamp()*1000)).encode('utf-8')


def register_dumpers(adapters: AdaptersMap):
    adapters.register_dumper(str, StringDumper)
    adapters.register_dumper(int, IntDumper)
    adapters.register_dumper(bool, BoolDumper)
    adapters.register_dumper(float, FloatDumper)
    adapters.register_dumper('datetime.datetime', DatetimeDumper)
