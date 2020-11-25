from __future__ import annotations

import asyncio
import contextvars
import functools
import typing as t
from types import TracebackType

import pypika as pk

from aerie.collections import Collection
from aerie.exceptions import DriverNotRegistered
from aerie.protocols import BaseDriver, IterableValues, Queryable
from aerie.url import URL
from aerie.utils import import_string

E = t.TypeVar("E", bound=type)

EntityFactoryType = t.Callable[[t.Mapping], t.Any]


def _make_entity(
    map_to: t.Type[E],
    entity_factory: EntityFactoryType,
    row: t.Mapping,
) -> t.Union[t.Mapping, E, t.Any]:
    assert not all(
        [
            map_to is not None,
            entity_factory is not None,
        ]
    ), 'Either "entity_factory" or "map_to" could be passed at same time.'

    if map_to:
        return map_to(**row)

    if entity_factory:
        return entity_factory(row)
    return row


class Database:
    drivers: t.Dict[str, t.Union[str, t.Type[BaseDriver]]] = {
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
        params: t.Mapping = None,
    ) -> t.Any:
        """Execute query with given params."""
        async with self.connection() as connection:
            return await connection.execute(stmt, params)

    async def execute_all(
        self,
        stmt: Queryable,
        params: t.List[t.Mapping] = None,
    ) -> t.Any:
        async with self.connection() as connection:
            return await connection.execute_all(stmt, params)

    async def fetch_one(
        self,
        stmt: Queryable,
        params: t.Mapping = None,
        map_to: t.Type[E] = None,
        entity_factory: EntityFactoryType = None,
    ) -> t.Optional[t.Union[t.Mapping, E, t.Any]]:
        factory = functools.partial(_make_entity, map_to, entity_factory)
        async with self.connection() as connection:
            row = await connection.fetch_one(str(stmt), params)
            if row:
                return factory(row)
            return None

    async def fetch_all(
        self,
        stmt: Queryable,
        params: t.Mapping = None,
        map_to: t.Type[E] = None,
        entity_factory: EntityFactoryType = None,
    ) -> t.Union[Collection[t.Mapping], Collection[E], t.Any]:
        factory = functools.partial(_make_entity, map_to, entity_factory)
        async with self.connection() as connection:
            rows = await connection.fetch_all(str(stmt), params)
            return Collection(list(map(factory, rows)))

    async def fetch_val(
        self, stmt: Queryable, params: t.Mapping = None, column: t.Any = 0
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
        map_to: t.Type[E] = None,
        entity_factory: EntityFactoryType = None,
    ) -> t.AsyncGenerator[t.Union[t.Mapping, E, t.Any], None]:
        factory = functools.partial(_make_entity, map_to, entity_factory)
        async with self.connection() as connection:
            async for row in connection.iterate(str(stmt), params):
                yield factory(row)

    async def insert(
        self,
        table_name: str,
        values: t.Mapping,
    ) -> int:
        qb = self.driver.insert_query(table_name, values)
        return await self.execute(qb, values)

    async def insert_all(
        self,
        table_name: str,
        values: IterableValues,
    ) -> None:
        qb = self.driver.insert_all_query(table_name, values)
        return await self.execute_all(qb, list(values))

    async def update(
        self,
        table_name: str,
        values: t.List[t.Mapping],
        where: t.Mapping = None,
    ) -> None:
        ...

    async def update_by(
        self,
        table_name: str,
        column: str,
        value: t.Any,
    ) -> None:
        ...

    async def delete(
        self,
        table_name: str,
        where: t.Mapping = None,
    ) -> None:
        ...

    async def delete_by(
        self,
        table_name: str,
        column: str,
        value: t.Any,
    ) -> None:
        ...

    async def upsert(
        self,
        table_name: str,
        values: t.Mapping,
        where: t.Mapping,
    ) -> None:
        ...

    def query_builder(self) -> pk.queries.Query:
        return self.driver.get_query_builder()

    def transaction(self, force_rollback: bool = False) -> _Transaction:
        """Manages database transactions (if supported by the used driver).
        It provides context manager interface and
        therefore may be used with `async with` keywords.

        >>> async with db.transaction():
        >>> # do stuff

        Also, for low-level control you can get a transaction instance
        and control its lifecycle by hand.

        >>> tx = db.transaction():
        >>> await tx.begin()
        >>> # do stuff
        >>> await tx.commit()

        There is a shortcut to begin the transaction when creating and object
        to avoid boilerplate:

        >>> tx = await db.transaction():
        >>> # do stuff
        >>> await tx.commit()
        """
        return _Transaction(self.connection, force_rollback)

    async def connect(self) -> Database:
        await self.driver.connect()
        return self

    async def disconnect(self) -> None:
        await self.driver.disconnect()

    def create_driver(self) -> BaseDriver:
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
    def __init__(self, driver: BaseDriver) -> None:
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
