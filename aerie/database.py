from __future__ import annotations

import contextvars
import functools
import typing as t

import pypika as pk

from aerie.collections import Collection
from aerie.connection import Connection, Transaction
from aerie.exceptions import DriverNotRegistered
from aerie.protocols import Queryable
from aerie.drivers.base.driver import BaseDriver, IterableValues
from aerie.terms import OnConflict
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
        default: t.Union[t.Mapping, E, t.Any] = None,
    ) -> t.Optional[t.Union[t.Mapping, E, t.Any]]:
        factory = functools.partial(_make_entity, map_to, entity_factory)
        async with self.connection() as connection:
            row = await connection.fetch_one(str(stmt), params)
            if row:
                return factory(row)
            return default

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
        on_conflict: str = OnConflict.RAISE,
        conflict_target: t.Union[str, t.List[str]] = None,
        replace_except: t.List[str] = None,
    ) -> int:
        qb = self.driver.insert_query(
            table_name,
            values,
            on_conflict,
            conflict_target,
            replace_except,
        )
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
        set: t.Mapping,
        where: t.Union[str, pk.terms.Term] = None,
        where_params: t.Mapping = None,
    ) -> None:
        qb = self.driver.update_query(table_name, set, where)
        where_params = where_params or {}
        await self.execute(qb, {**set, **where_params})

    async def delete(
        self,
        table_name: str,
        where: t.Union[str, pk.terms.Term] = None,
        where_params: t.Mapping = None,
    ) -> None:
        qb = self.driver.delete_query(table_name, where)
        await self.execute(qb, where_params)

    async def upsert(
        self,
        table_name: str,
        values: t.Mapping,
        where: t.Mapping,
    ) -> None:
        ...

    async def count(
        self,
        table_name: str,
        column: str = "*",
        where: t.Union[str, pk.terms.Term] = None,
        where_params: t.Mapping = None,
    ) -> int:
        qb = self.driver.count_query(table_name, column, where)
        return await self.fetch_val(qb, where_params)

    def query_builder(self) -> pk.queries.Query:
        return self.driver.get_query_builder()

    def transaction(self, force_rollback: bool = False) -> Transaction:
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
        return Transaction(self.connection, force_rollback)

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

    def connection(self) -> Connection:
        try:
            return self._connection_context.get()
        except LookupError:
            connection = Connection(self.driver)
            self._connection_context.set(connection)
            return connection

    async def __aenter__(self) -> Database:
        return await self.connect()

    async def __aexit__(self, *args) -> None:
        await self.disconnect()

    def __repr__(self) -> str:
        return f"<Database: {self.url}>"
