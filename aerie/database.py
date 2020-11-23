from __future__ import annotations

import asyncio
import contextvars
import typing as t
from types import TracebackType

from aerie.exceptions import DriverNotRegistered
from aerie.protocols import Driver, Queryable
from aerie.url import URL
from aerie.utils import import_string

E = t.TypeVar("E")


class Database:
    drivers: t.Dict[str, t.Union[str, t.Type[Driver]]] = {
        "sqlite": "aerie.drivers.sqlite.SQLiteDriver",
        "postgres": "aerie.drivers.postgresql.PostgresDriver",
        "postgresql": "aerie.drivers.postgresql.PostgresDriver",
    }

    def __init__(self, url: t.Union[str, URL]) -> None:
        self.url = URL(url) if isinstance(url, str) else url
        self.driver = self.create_driver()
        self._connection_context: contextvars.ContextVar = contextvars.ContextVar(
            "connection"
        )

    async def execute(
        self,
        stmt: Queryable,
        params: dict = None,
    ) -> t.Any:
        """Execute query with given params."""
        async with self.connection() as connection:
            return await connection.execute(stmt, params)

    async def execute_all(
        self,
        stmt: Queryable,
        params: t.Optional[t.List[t.Mapping]] = None,
    ) -> t.Any:
        async with self.connection() as connection:
            return await connection.execute_all(stmt, params)

    async def fetch_one(
        self,
        stmt: Queryable,
        params: t.Optional[t.Mapping] = None,
    ) -> t.Any:
        async with self.connection() as connection:
            return await connection.fetch_one(str(stmt), params)

    async def fetch_all(
        self,
        stmt: Queryable,
        params: t.Optional[t.Mapping] = None,
    ) -> t.Any:
        async with self.connection() as connection:
            return await connection.fetch_all(str(stmt), params)

    async def fetch_val(
        self, stmt: Queryable, params: t.Optional[t.Mapping] = None, column: t.Any = 0
    ) -> t.Any:
        async with self.connection() as connection:
            row = await connection.fetch_one(str(stmt), params)
            if row is None:
                return row
            return row[column]

    async def iterate(
        self,
        stmt: Queryable,
        params: t.Optional[t.Mapping] = None,
    ) -> t.AsyncGenerator[t.Any, None]:
        async with self.connection() as connection:
            async for row in connection.iterate(str(stmt), params):
                yield row

    def transaction(self, force_rollback: bool = False) -> _Transaction:
        return _Transaction(self.connection, force_rollback)

    async def connect(self) -> Database:
        await self.driver.connect()
        return self

    async def disconnect(self) -> None:
        await self.driver.disconnect()

    def create_driver(self) -> Driver:
        if self.url.scheme not in self.drivers:
            raise DriverNotRegistered(
                f'No driver for scheme "{self.url.scheme}". '
                "Use Database.register_driver() to register a new driver."
            )
        driver_class_spec = self.drivers[self.url.scheme]
        if isinstance(driver_class_spec, str):
            driver_class = import_string(driver_class_spec)
        else:
            driver_class = driver_class_spec
        return driver_class(self.url)

    @classmethod
    def register_driver(cls, scheme: str, driver: str) -> None:
        cls.drivers[scheme] = driver

    def connection(self) -> _Connection:
        try:
            return self._connection_context.get()
        except LookupError:
            connection = _Connection(self.driver)
            self._connection_context.set(connection)
            return connection

    async def __aenter__(self) -> Database:
        return await self.connect()

    async def __aexit__(self, *args) -> None:
        await self.disconnect()

    def __repr__(self) -> str:
        return f"<Database: {self.url}>"


class _Connection:
    def __init__(self, driver: Driver) -> None:
        self._driver = driver
        self._connection = driver.connection()
        self._query_lock = asyncio.Lock()
        self._connection_lock = asyncio.Lock()
        self._connection_counter = 0

        self.transaction_lock = asyncio.Lock()
        self.transaction_counter = 0
        self.transactions: t.List[_Transaction] = []

    async def execute(self, stmt: Queryable, params: t.Mapping = None) -> t.Any:
        async with self._query_lock:
            return await self._connection.execute(str(stmt), params)

    async def execute_all(
        self,
        stmt: Queryable,
        params: t.Optional[t.List[t.Mapping]] = None,
    ) -> t.Any:
        async with self._query_lock:
            return await self._connection.execute_all(str(stmt), params)

    async def fetch_one(
        self,
        stmt: str,
        params: t.Optional[t.Mapping] = None,
    ) -> t.Mapping:
        async with self._query_lock:
            return await self._connection.fetch_one(str(stmt), params)

    async def fetch_all(
        self,
        stmt: str,
        params: t.Optional[t.Mapping] = None,
    ) -> t.List[t.Mapping]:
        async with self._query_lock:
            return await self._connection.fetch_all(stmt, params)

    async def fetch_val(
        self, stmt: str, params: t.Optional[t.Mapping] = None, column: t.Any = 0
    ) -> t.Any:
        async with self._query_lock:
            return await self._connection.fetch_val(stmt, params, column)

    async def iterate(
        self,
        stmt: str,
        params: t.Optional[t.Mapping] = None,
    ) -> t.AsyncGenerator[t.Any, None]:
        async with self._query_lock:
            async for row in self._connection.iterate(stmt, params):
                yield row

    def transaction(self) -> _Transaction:
        return _Transaction(lambda: self)

    async def __aenter__(self) -> _Connection:
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


class _Transaction:
    def __init__(
        self,
        connection_factory: t.Callable[[], _Connection],
        force_rollback: bool = False,
    ) -> None:
        self._connection = connection_factory()
        self._transaction = self._connection._connection.transaction()
        self._force_rollback = force_rollback

    @property
    def _is_root(self) -> bool:
        return not self._connection.transaction_counter

    async def begin(self) -> _Transaction:
        async with self._connection.transaction_lock:
            await self._connection.__aenter__()
            await self._transaction.begin(self._is_root)
            self._connection.transaction_counter += 1
            self._connection.transactions.append(self)
            return self

    async def commit(self) -> None:
        async with self._connection.transaction_lock:
            self._connection.transaction_counter -= 1
            self._connection.transactions.remove(self)
            await self._transaction.commit()
            await self._connection.__aexit__(None, None, None)

    async def rollback(self) -> None:
        async with self._connection.transaction_lock:
            self._connection.transaction_counter -= 1
            self._connection.transactions.remove(self)
            await self._transaction.rollback()
            await self._connection.__aexit__(None, None, None)

    async def __aenter__(self) -> _Transaction:
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
