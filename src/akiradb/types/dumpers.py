from decimal import Decimal
from datetime import datetime
import re
from psycopg.adapt import Dumper, PyFormat, RecursiveDumper
from psycopg.pq import Format
from psycopg.adapt import AdaptersMap

from akiradb.types.query import Label


forbidden_chars = re.compile('[^a-zA-Z0-9_.]')


class StringDumper(Dumper):
    format = Format.TEXT

    def dump(self, obj: str) -> bytes:
        return obj.encode('utf-8')


class LabelDumper(Dumper):
    format = Format.TEXT

    def dump(self, obj: Label) -> bytes:
        return obj.label_name.encode('utf-8')

    def quote(self, obj: Label) -> bytes:
        return forbidden_chars.sub('_', obj.label_name).encode('utf-8')


class IntDumper(Dumper):
    format = Format.TEXT

    def dump(self, obj: int) -> bytes:
        return repr(obj).encode('utf-8')

    def quote(self, obj: int) -> bytes:
        return self.dump(obj)


class BoolDumper(Dumper):
    format = Format.TEXT

    def dump(self, obj: bool) -> bytes:
        return ("true" if obj else "false").encode('utf-8')

    def quote(self, obj: bool) -> bytes:
        return self.dump(obj)


class FloatDumper(Dumper):
    format = Format.TEXT

    def dump(self, obj: float) -> bytes:
        return format(Decimal(obj), 'f').encode('utf-8')

    def quote(self, obj: float) -> bytes:
        return self.dump(obj)


class DatetimeDumper(Dumper):
    format = Format.TEXT

    def dump(self, obj: datetime) -> bytes:
        return repr(int(obj.timestamp()*1000)).encode('utf-8')


class DictDumper(RecursiveDumper):
    format = Format.TEXT

    def dump(self, _: dict) -> bytes:
        return '{}'.encode('utf-8')

    def quote(self, obj: dict) -> bytes:
        from akiradb.model.proxies import PropertyChangesRecorder

        format = PyFormat.from_pq(self.format)
        res = b''
        first = True
        for (key, value) in obj.items():
            if not first:
                res += b','
            if isinstance(value, PropertyChangesRecorder):
                value = value.value
            res += (forbidden_chars.sub('_', key).encode('utf-8') + b':'
                    + self._tx.get_dumper(value, format).quote(value))
            first = False
        return b'{' + res + b'}'


def register_dumpers(adapters: AdaptersMap):
    adapters.register_dumper(str, StringDumper)
    adapters.register_dumper(Label, LabelDumper)
    adapters.register_dumper(int, IntDumper)
    adapters.register_dumper(bool, BoolDumper)
    adapters.register_dumper(float, FloatDumper)
    adapters.register_dumper(dict, DictDumper)
    adapters.register_dumper('datetime.datetime', DatetimeDumper)
