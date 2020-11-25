from __future__ import annotations

import itertools
import typing as t
from types import TracebackType

import asyncpg
import asyncpg.pool
import pypika as pk

from aerie.exceptions import UniqueViolationError
from aerie.protocols import BaseConnection, BaseDriver, BaseTransaction
from aerie.terms import OnConflict
from aerie.url import URL


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


class PostgresDriver(
    BaseDriver[pk.dialects.PostgreSQLQuery, pk.dialects.PostgreQueryBuilder]
):
    dialect = "postgresql"
    query_class = pk.dialects.PostgreSQLQuery

    def __init__(self, url: URL) -> None:
        self.url = url
        self.pool: t.Optional[asyncpg.pool.Pool] = None

    async def connect(self) -> None:
        self.pool = asyncpg.create_pool(
            self.url.url,
            **self.url.options,
        )

    async def disconnect(self) -> None:
        assert self.pool, "Driver is not connected."
        await self.pool.close()
        self.pool = None

    def connection(self) -> _Connection:
        return _Connection(self.pool)

    def insert_query(
        self,
        table_name: str,
        values: t.Mapping,
        on_conflict: t.Union[str, t.List[str]] = OnConflict.RAISE,
        conflict_target: t.Union[str, t.List[str]] = None,
        replace_except: t.List[str] = None,
    ) -> pk.dialects.PostgreQueryBuilder:
        qb = super().insert_query(table_name, values)
        conflict_target = conflict_target or []
        replace_except = replace_except or []

        if on_conflict != OnConflict.RAISE:
            qb = qb.on_conflict(*conflict_target)

        if on_conflict == OnConflict.NOTHING:
            qb = qb.do_nothing()

        if on_conflict == OnConflict.REPLACE:
            for column, value in values.items():
                if column in replace_except:
                    continue
                qb = qb.do_update(column, value)
        return qb
