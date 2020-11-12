from __future__ import annotations

import typing as t

from aerie.exceptions import DriverNotRegistered
from aerie.protocols import Driver, Queryable, Row, Transaction
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

    async def execute(
            self, stmt: Queryable, params: t.Mapping = None,
    ) -> t.Any:
        """Execute query with given params."""
        async with self.driver.connect() as connection:
            return await connection.execute(stmt, params)

    async def execute_many(
            self, stmt: Queryable, params: t.List[t.Mapping] = None,
    ) -> t.Any:
        async with self.driver.connect() as connection:
            return await connection.execute_many(stmt, params)

    async def fetch_one(
            self, stmt: Queryable, params: t.Mapping = None,
    ) -> t.Any:
        async with self.driver.connect() as connection:
            return await connection.fetch_one(stmt, params)

    async def fetch_all(
            self, stmt: Queryable, params: t.Mapping = None,
    ) -> t.Any:
        async with self.driver.connect() as connection:
            return await connection.fetch_all(stmt, params)

    async def fetch_val(
            self, stmt: Queryable, params: t.Mapping = None,
    ) -> t.Any:
        async with self.driver.connect() as connection:
            return await connection.fetch_val(stmt, params)

    async def iterate(
            self, stmt: Queryable, params: t.Mapping = None,
    ) -> t.AsyncGenerator[t.Mapping, None]:
        async with self.driver.connect() as connection:
            return await connection.iterate(stmt, params)

    async def transaction(self) -> Transaction:
        async with self.driver.connect() as connection:
            return await connection.transaction()

    async def connect(self):
        await self.driver.connect()
        return self

    async def disconnect(self):
        await self.driver.disconnect()

    # async def fetch_one(
    #         self, stmt: Queryable,
    #         params: t.Mapping = None,
    #         map_to: t.Type[E] = None,
    # ) -> t.Optional[t.Union[E, Row]]:
    #     async with self.pool.acquire() as connection:
    #         results = await connection.fetch_all(stmt, params)
    #         if not results or not len(results):
    #             return None
    #
    #         result = results[0]
    #         if map_to:
    #             result = mapper(result, map_to)
    #         return result
    #
    # async def fetch_all(
    #         self,
    #         stmt: Queryable,
    #         params: t.Mapping = None,
    #         map_to: t.Type = None,
    # ):
    #     async with self.pool.acquire() as connection:
    #         results = await connection.fetch_all(stmt, params)

    # async def scalar(self, stmt: Queryable):
    #     pass

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

    async def __aenter__(self) -> Database:
        return await self.connect()

    async def __aexit__(self, *args):
        await self.disconnect()

    def __repr__(self) -> str:
        return f'<Database: {self.url}>'
