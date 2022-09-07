from datetime import datetime
from psycopg.adapt import Dumper
from psycopg.pq import Format


class DatetimeDumper(Dumper):
    format = Format.TEXT

    def dump(self, obj: datetime) -> bytes:
        return repr(int(obj.timestamp()*1000)).encode('utf-8')
