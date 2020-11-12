from __future__ import annotations

import typing as t
import uuid
from contextlib import asynccontextmanager

import aiosqlite

from aerie.protocols import BaseConnection, BaseSavePoint, BaseTransaction
from aerie.url import URL


class SQLiteDriver:
    def __init__(self, url: URL):
        self.url = url

    @asynccontextmanager
    async def connect(self) -> aiosqlite.Connection:
        async with aiosqlite.connect(
                database=self.url.db_name,
                isolation_level=None,
                **self.url.options,
        ) as db:
            db.row_factory = aiosqlite.Row
            yield _Connection(db)


class _Connection(BaseConnection):
    def __init__(self, conn: aiosqlite.Connection):
        self._connection = conn

    async def execute(self, stmt: str, params: t.Mapping = None) -> t.Any:
        cursor = await self._connection.execute(stmt, params)
        if cursor.lastrowid == 0:
            return cursor.rowcount
        return cursor.lastrowid

    async def execute_many(
            self, stmt: str, params: t.List[t.Mapping] = None,
    ) -> t.Any:
        async with self._connection.executemany(stmt, params) as cursor:
            if cursor.lastrowid == 0:
                return cursor.rowcount
            return cursor.lastrowid

    async def fetch_one(
            self, stmt: str, params: t.Mapping = None,
    ) -> t.Mapping:
        async with self._connection.execute(stmt, params) as cursor:
            return await cursor.fetchone()

    async def fetch_all(
            self, stmt: str, params: t.Mapping = None,
    ) -> t.List[t.Mapping]:
        async with self._connection.execute(stmt, params) as cursor:
            return await cursor.fetchall()

    async def fetch_val(self, stmt: str, params: t.Mapping = None) -> t.Any:
        async with self._connection.execute(stmt, params) as cursor:
            row = await cursor.fetchone()
            if row:
                return row[0]

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


class _SavePoint(BaseSavePoint):
    def __init__(self, connection: _Connection, name: str = None):
        self.connection = connection
        self.name = name or 'savepoint_' + str(uuid.uuid4())

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
        self.is_root = True

    async def begin(self) -> _Transaction:
        await self.connection.execute('BEGIN')
        return self

    async def commit(self):
        await self.connection.raw_connection.commit()

    async def rollback(self):
        await self.connection.raw_connection.rollback()

    def savepoint(self, name: str = None) -> _SavePoint:
        return _SavePoint(self.connection, name)
