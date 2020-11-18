from __future__ import annotations

import asyncio
import contextvars
import typing as t

from aerie.exceptions import DriverNotRegistered
from aerie.protocols import Driver, Queryable, Row
from aerie.url import URL
from aerie.utils import import_string

E = t.TypeVar('E')


def mapper(row: Row, klass: t.Type[E]) -> E:
    return klass(**row)


class Database:
    drivers: t.Dict[str, str] = {
        'sqlite': 'aerie.drivers.sqlite.SQLiteDriver',
    }

    def __init__(self, url: t.Union[str, URL]):
        self.url = URL(url) if isinstance(url, str) else url
        self.driver = self.create_driver()
        self._connection_context = contextvars.ContextVar('connection')

    async def execute(
            self, stmt: Queryable, params: t.Mapping = None,
    ) -> t.Any:
        """Execute query with given params."""
        async with self.connection() as connection:
            return await connection.execute(stmt, params)

    async def execute_all(
            self, stmt: Queryable, params: t.List[t.Mapping] = None,
    ) -> t.Any:
        async with self.connection() as connection:
            return await connection.execute_all(stmt, params)

    async def fetch_one(
            self, stmt: Queryable, params: t.Mapping = None,
    ) -> t.Any:
        async with self.connection() as connection:
            return await connection.fetch_one(stmt, params)

    async def fetch_all(
            self, stmt: Queryable, params: t.Mapping = None,
    ) -> t.Any:
        async with self.connection() as connection:
            return await connection.fetch_all(stmt, params)

    async def fetch_val(
            self, stmt: Queryable, params: t.Mapping = None, column: t.Any = 0
    ) -> t.Any:
        async with self.connection() as connection:
            row = await connection.fetch_one(stmt, params)
            if row is None:
                return row
            return row[column]

    async def iterate(
            self, stmt: Queryable, params: t.Mapping = None,
    ) -> t.AsyncGenerator[t.Mapping, None]:
        async with self.connection() as connection:
            async for row in connection.iterate(stmt, params):
                yield row

    def transaction(self) -> _Transaction:
        return _Transaction(self.connection)

    async def connect(self):
        await self.driver.connect()
        return self

    async def disconnect(self):
        await self.driver.disconnect()

    def create_driver(self) -> Driver:
        if self.url.scheme not in self.drivers:
            raise DriverNotRegistered(
                f'No driver for scheme "{self.url.scheme}". '
                'Use Database.register_driver() to register a new driver.'
            )
        driver_class = self.drivers[self.url.scheme]
        if isinstance(driver_class, str):
            driver_class = import_string(driver_class)
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

    async def __aexit__(self, *args):
        await self.disconnect()

    def __repr__(self) -> str:
        return f'<Database: {self.url}>'


class _Connection:
    def __init__(self, driver: Driver):
        self._driver = driver
        self._connection = driver.connection()
        self._query_lock = asyncio.Lock()
        self._connection_lock = asyncio.Lock()
        self._connection_counter = 0

        self.transaction_lock = asyncio.Lock()
        self.transaction_counter = 0
        self.transactions = []

    async def execute(self, stmt: Queryable, params: t.Mapping) -> t.Any:
        async with self._query_lock:
            return await self._connection.execute(stmt, params)

    async def execute_all(
            self, stmt: Queryable, params: t.List[t.Mapping],
    ) -> t.Any:
        async with self._query_lock:
            return await self._connection.execute_all(stmt, params)

    async def fetch_one(
            self, stmt: str, params: t.Mapping = None,
    ) -> t.Mapping:
        async with self._query_lock:
            return await self._connection.fetch_one(stmt, params)

    async def fetch_all(
            self, stmt: str, params: t.Mapping = None,
    ) -> t.List[t.Mapping]:
        async with self._query_lock:
            return await self._connection.fetch_all(stmt, params)

    async def fetch_val(
            self, stmt: str, params: t.Mapping = None, column: t.Any = 0
    ) -> t.Any:
        async with self._query_lock:
            return await self._connection.fetch_val(stmt, params, column)

    async def iterate(
            self, stmt: str, params: t.Mapping = None,
    ) -> t.AsyncGenerator[t.Mapping, None]:
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
    def __init__(self, connection_factory: t.Callable[[], _Connection]):
        self._connection = connection_factory()
        self._transaction = self._connection._connection.transaction()

    @property
    def _is_root(self):
        return not self._connection.transaction_counter

    async def begin(self) -> _Transaction:
        async with self._connection.transaction_lock:
            await self._connection.__aenter__()
            await self._transaction.begin(self._is_root)
            self._connection.transaction_counter += 1
            return self

    async def commit(self):
        async with self._connection.transaction_lock:
            self._connection.transaction_counter -= 1
            await self._transaction.commit()
            await self._connection.__aexit__(None, None, None)

    async def rollback(self):
        async with self._connection.transaction_lock:
            self._connection.transaction_counter -= 1
            await self._transaction.rollback()
            await self._connection.__aexit__(None, None, None)

    async def __aenter__(self):
        return await self.begin()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            await self.rollback()
        else:
            await self.commit()
