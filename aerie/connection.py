from __future__ import annotations

import asyncio
import typing as t
from types import TracebackType

from aerie.protocols import Queryable
from aerie.drivers.base.driver import BaseDriver


class Connection:
    def __init__(self, driver: BaseDriver) -> None:
        self._driver = driver
        self._connection = driver.connection()
        self._query_lock = asyncio.Lock()
        self._connection_lock = asyncio.Lock()
        self._connection_counter = 0

        self.transaction_lock = asyncio.Lock()
        self.transaction_counter = 0
        self.transactions: t.List[Transaction] = []

    async def execute(self, stmt: Queryable, params: t.Mapping = None) -> t.Any:
        async with self._query_lock:
            return await self._connection.execute(str(stmt), params)

    async def execute_all(
        self,
        stmt: Queryable,
        params: t.List[t.Mapping] = None,
    ) -> t.Any:
        async with self._query_lock:
            return await self._connection.execute_all(str(stmt), params)

    async def fetch_one(
        self,
        stmt: str,
        params: t.Mapping = None,
    ) -> t.Optional[t.Mapping]:
        async with self._query_lock:
            return await self._connection.fetch_one(str(stmt), params)

    async def fetch_all(
        self,
        stmt: str,
        params: t.Mapping = None,
    ) -> t.List[t.Mapping]:
        async with self._query_lock:
            return await self._connection.fetch_all(stmt, params)

    async def fetch_val(
        self, stmt: str, params: t.Mapping = None, column: t.Any = 0
    ) -> t.Any:
        async with self._query_lock:
            return await self._connection.fetch_val(stmt, params, column)

    async def iterate(
        self,
        stmt: str,
        params: t.Mapping = None,
    ) -> t.AsyncGenerator[t.Any, None]:
        async with self._query_lock:
            async for row in self._connection.iterate(stmt, params):
                yield row

    def transaction(self) -> Transaction:
        return Transaction(lambda: self)

    async def __aenter__(self) -> Connection:
        async with self._connection_lock:
            self._connection_counter += 1
            if self._connection_counter == 1:
                await self._connection.acquire()
        return self

    async def __aexit__(self, *args) -> None:
        async with self._connection_lock:
            assert self._connection is not None
            self._connection_counter -= 1
            if self._connection_counter == 0:
                await self._connection.release()


class Transaction:
    def __init__(
        self,
        connection_factory: t.Callable[[], Connection],
        force_rollback: bool = False,
    ) -> None:
        self._connection = connection_factory()
        self._transaction = self._connection._connection.transaction()
        self._force_rollback = force_rollback

    @property
    def _is_root(self) -> bool:
        return not self._connection.transaction_counter

    async def begin(self) -> Transaction:
        """Begin transaction."""
        async with self._connection.transaction_lock:
            await self._connection.__aenter__()
            await self._transaction.begin(self._is_root)
            self._connection.transaction_counter += 1
            self._connection.transactions.append(self)
            return self

    async def commit(self) -> None:
        """Commit the transaction."""
        async with self._connection.transaction_lock:
            self._connection.transaction_counter -= 1
            self._connection.transactions.remove(self)
            await self._transaction.commit()
            await self._connection.__aexit__(None, None, None)

    async def rollback(self) -> None:
        """Rollback the transaction."""
        async with self._connection.transaction_lock:
            self._connection.transaction_counter -= 1
            self._connection.transactions.remove(self)
            await self._transaction.rollback()
            await self._connection.__aexit__(None, None, None)

    def __await__(self) -> t.Generator:
        """Creates new transaction instance and automatically begins it.
        You still have to control commit() or rollback() calls.

        Example:
            tx = await db.transaction() # excutes BEGIN under the hood
            tx.commit()
        """
        return self.begin().__await__()

    async def __aenter__(self) -> Transaction:
        return await self.begin()

    async def __aexit__(
        self,
        exc_type: t.Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        if self in self._connection.transactions:
            if exc_type is not None or self._force_rollback:
                await self.rollback()
            else:
                await self.commit()
