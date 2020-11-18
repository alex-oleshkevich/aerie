from __future__ import annotations

import typing as t
import uuid

import aiosqlite

from aerie.protocols import BaseConnection, BaseSavePoint, BaseTransaction
from aerie.url import URL


class _Pool:
    def __init__(self, url: URL, **options: t.Any) -> None:
        self._url = url
        self._options = options

    async def acquire(self) -> aiosqlite.Connection:
        connection = aiosqlite.connect(
            database=self._url.db_name, isolation_level=None, **self._options
        )
        await connection.__aenter__()
        connection.row_factory = aiosqlite.Row
        return connection

    async def release(self, connection: aiosqlite.Connection) -> None:
        await connection.__aexit__(None, None, None)


class SQLiteDriver:
    def __init__(self, url: URL):
        self.url = url
        self.pool = _Pool(url)

    async def connect(self): pass

    async def disconnect(self): pass

    def connection(self) -> _Connection:
        return _Connection(self.pool)


class _Connection(BaseConnection):
    def __init__(self, pool: _Pool):
        self._pool = pool
        self._connection = None  # type: aiosqlite.Connection

    async def acquire(self):
        self._connection = await self._pool.acquire()

    async def release(self):
        await self._pool.release(self._connection)

    async def execute(self, stmt: str, params: t.Mapping = None) -> t.Any:
        cursor = await self._connection.execute(stmt, params)
        if cursor.lastrowid == 0:
            return cursor.rowcount
        return cursor.lastrowid

    async def execute_all(
            self, stmt: str, params: t.List[t.Mapping] = None,
    ) -> t.Any:
        async with self._connection.executemany(stmt, params) as cursor:
            if cursor.lastrowid == 0:
                return cursor.rowcount
            return cursor.lastrowid

    async def fetch_one(
            self, stmt: str, params: t.Mapping = None,
    ) -> t.Optional[t.Mapping]:
        async with self._connection.execute(stmt, params) as cursor:
            return await cursor.fetchone()

    async def fetch_all(
            self, stmt: str, params: t.Mapping = None,
    ) -> t.List[t.Mapping]:
        async with self._connection.execute(stmt, params) as cursor:
            return await cursor.fetchall()

    async def iterate(
            self, stmt: str, params: t.Mapping = None,
    ) -> t.AsyncGenerator[t.Mapping, None]:
        async with self._connection.execute(stmt, params) as cursor:
            async for row in cursor:
                yield row

    def transaction(self) -> _Transaction:
        return _Transaction(self)

    @property
    def raw_connection(self) -> aiosqlite.Connection:
        return self._connection

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, *args):
        await self.release()


class _SavePoint(BaseSavePoint):
    def __init__(self, connection: _Connection, name: str = None):
        self.connection = connection
        self.name = name or 'aerie_' + str(uuid.uuid4()).replace('-', '')

    async def begin(self) -> _SavePoint:
        await self.connection.execute(f'SAVEPOINT {self.name}')
        return self

    async def commit(self):
        await self.connection.execute(f'RELEASE SAVEPOINT {self.name}')

    async def rollback(self):
        await self.connection.execute(f'ROLLBACK TO SAVEPOINT {self.name}')


class _Transaction(BaseTransaction):
    def __init__(self, connection: _Connection):
        self.connection = connection
        self._savepoint = _SavePoint(self.connection)
        self._is_root = True

    async def begin(self, is_root: bool = True) -> _Transaction:
        self._is_root = is_root
        if self._is_root:
            await self.connection.execute('BEGIN')
        else:
            await self._savepoint.begin()
        return self

    async def commit(self) -> None:
        if self._is_root:
            await self.connection.execute('COMMIT')
        else:
            await self._savepoint.commit()

    async def rollback(self) -> None:
        if self._is_root:
            await self.connection.execute('ROLLBACK')
        else:
            await self._savepoint.rollback()
