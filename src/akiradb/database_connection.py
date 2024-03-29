from asyncio import Lock
import contextlib
from typing import AsyncGenerator, Optional, cast

import psycopg
from akiradb.exceptions import AkiraNotConnectedException
from akiradb.types import loaders, dumpers
from akiradb.types.query import Params, Query


class AkiraAsyncClientCursor(psycopg.AsyncClientCursor):
    async def execute_sql(self: psycopg.AsyncClientCursor._Self, query: Query,
                          params: Optional[Params] = None) -> psycopg.AsyncClientCursor._Self:
        return await self.execute(f'{query};', params)

    async def execute_cypher(self: psycopg.AsyncClientCursor._Self, query: Query,
                             params: Optional[Params] = None) -> psycopg.AsyncClientCursor._Self:
        return await self.execute(f'{{cypher}} {query};', params)


class DatabaseConnection():
    def __init__(self, hostname='localhost', port=5432, database='test_db',
                 username='user', password='password'):
        self.hostname = hostname
        self.port = port
        self.database = database
        self.user = username
        self.password = password

        self._conn = None
        self._conn_transaction_lock = Lock()

    async def connect(self):
        self._conn = await psycopg.AsyncConnection.connect(
            f"dbname={self.database} user={self.user} password={self.password} "
            f"host={self.hostname}", autocommit=True, cursor_factory=AkiraAsyncClientCursor)
        self._conn.prepare_threshold = None
        loaders.register_loaders(self._conn.adapters)
        dumpers.register_dumpers(self._conn.adapters)

    @contextlib.asynccontextmanager
    async def execute(self, command):
        if not self._conn:
            raise AkiraNotConnectedException()

        async with self._conn.cursor() as cur:
            await cur.execute(command)
            yield cur

    @contextlib.asynccontextmanager
    async def pipeline(self):
        if not self._conn:
            raise AkiraNotConnectedException()

        async with self._conn.pipeline() as pipeline:
            yield pipeline

    @contextlib.asynccontextmanager
    async def cursor(self, **kwargs) -> AsyncGenerator[AkiraAsyncClientCursor, None]:
        if not self._conn:
            raise AkiraNotConnectedException()

        # Only a single transaction per connection
        async with self._conn_transaction_lock:
            async with self._conn.transaction():
                async with self._conn.cursor(**kwargs) as cur:
                    yield cast(AkiraAsyncClientCursor, cur)

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
