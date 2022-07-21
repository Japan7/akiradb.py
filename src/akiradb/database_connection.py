import contextlib

import psycopg

from .exceptions.not_connected_exception import AkiraNotConnectedException


class DatabaseConnection():
    def __init__(self, hostname='localhost', port=5432, database='test_db',
                 username='user', password='password'):
        self.hostname = hostname
        self.port = port
        self.database = database
        self.user = username
        self.password = password

        self._conn = None

    async def connect(self):
        self._conn = await psycopg.AsyncConnection.connect(
            f"dbname={self.database} user={self.user} password={self.password} "
            f"host={self.hostname}", autocommit=True)

    @contextlib.asynccontextmanager
    async def execute(self, command):
        if not self._conn:
            raise AkiraNotConnectedException()

        async with self._conn.cursor() as cur:
            await cur.execute(command)
            yield cur

    async def commit(self):
        if not self._conn:
            raise AkiraNotConnectedException()

        async with self.execute('commit;'):
            pass

    async def close(self):
        if not self._conn:
            raise AkiraNotConnectedException()

        await self._conn.close()
        self._conn = None
