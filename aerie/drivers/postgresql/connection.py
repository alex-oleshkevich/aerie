from __future__ import annotations

import itertools
import typing as t
from types import TracebackType

import asyncpg

from aerie.drivers.base.connection import BaseConnection, BaseTransaction
from aerie.drivers.base.driver import BaseDriver
from aerie.exceptions import UniqueViolationError


class _Transaction(BaseTransaction):
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    async def begin(self, is_root: bool = True) -> _Transaction:
        self._tx = self.connection.raw_connection.transaction()
        await self._tx.start()
        return self

    async def commit(self) -> None:
        await self._tx.commit()

    async def rollback(self) -> None:
        await self._tx.rollback()

    async def __aenter__(self):
        return await self.begin()

    async def __aexit__(
        self,
        exc_type: t.Tuple[BaseException],
        exc_val: BaseDriver,
        exc_tb: TracebackType,
    ):
        if exc_type is not None:
            await self.rollback()
        else:
            await self.commit()


class _Connection(BaseConnection):
    def __init__(self, pool: asyncpg.pool.Pool) -> None:
        self._pool = pool
        self._connection: t.Optional[asyncpg.connection.Connection] = None

    async def acquire(self) -> None:
        await self._pool
        self._connection = await self._pool.acquire()

    async def release(self) -> None:
        await self._pool.release(self._connection)

    async def execute(self, stmt: str, params: t.Mapping = None) -> t.Any:
        assert self._connection is not None, "Connection is not acquired."
        stmt, args = self._replace_placeholders(stmt, params)
        try:
            return await self._connection.fetchval(stmt, *args)
        except asyncpg.UniqueViolationError as ex:
            raise UniqueViolationError(str(ex)) from None

    async def execute_all(
        self,
        stmt: str,
        params: t.List[t.Mapping] = None,
    ) -> t.Any:
        assert self._connection is not None, "Connection is not acquired."
        _args = []
        if params:
            for index, data in enumerate(params):
                if index == 0:
                    stmt, args_0 = self._replace_placeholders(stmt, data)
                    _args.append(args_0)
                else:
                    _args.append(list(data.values()))

        try:
            return await self._connection.executemany(stmt, _args)
        except asyncpg.UniqueViolationError as ex:
            raise UniqueViolationError(str(ex)) from None

    async def fetch_one(
        self,
        stmt: str,
        params: t.Mapping = None,
    ) -> t.Optional[t.Mapping]:
        assert self._connection is not None, "Connection is not acquired."
        stmt, args = self._replace_placeholders(stmt, params)
        return await self._connection.fetchrow(stmt, *args)

    async def fetch_all(
        self,
        stmt: str,
        params: t.Mapping = None,
    ) -> t.List[t.Mapping]:
        assert self._connection is not None, "Connection is not acquired."
        stmt, args = self._replace_placeholders(stmt, params)
        return await self._connection.fetch(stmt, *args)

    async def iterate(
        self,
        stmt: str,
        params: t.Mapping = None,
    ) -> t.AsyncGenerator[t.Any, None]:
        assert self._connection is not None, "Connection is not acquired."
        stmt, args = self._replace_placeholders(stmt, params)
        async with self.transaction():
            async for row in self._connection.cursor(stmt, *args):
                yield row

    def transaction(self) -> _Transaction:
        return _Transaction(self)

    @property
    def raw_connection(self) -> asyncpg.Connection:
        return self._connection

    async def __aenter__(self) -> "_Connection":
        await self.acquire()
        return self

    async def __aexit__(self, *args) -> None:
        await self.release()

    def _replace_placeholders(
        self,
        stmt: str,
        params: t.Mapping = None,
    ) -> t.Tuple[str, t.List]:
        args = []
        if params:
            counter = itertools.count(1)
            for key, value in params.items():
                index = next(counter)
                stmt = stmt.replace(f":{key}", f"${index}")
                args.append(value)
        return stmt, args
