from __future__ import annotations

import typing as t


class Stringable(t.Protocol):
    def __str__(self) -> str:
        ...


Queryable = t.Union[str, Stringable]


class Row(t.Mapping):
    pass


class BaseConnection:
    async def acquire(self) -> None:
        raise NotImplementedError()

    async def release(self) -> None:
        raise NotImplementedError()

    async def execute(self, stmt: str, params: t.Optional[t.Mapping] = None) -> t.Any:
        raise NotImplementedError()

    async def execute_all(
        self,
        stmt: str,
        params: t.Optional[t.List[t.Mapping]] = None,
    ) -> t.Any:
        raise NotImplementedError()

    async def fetch_one(
        self,
        stmt: str,
        params: t.Optional[t.Mapping] = None,
    ) -> t.Optional[t.Mapping]:
        raise NotImplementedError()

    async def fetch_val(
        self, stmt: str, params: t.Optional[t.Mapping] = None, column: t.Any = 0
    ) -> t.Optional[t.Mapping]:
        row = await self.fetch_one(stmt, params)
        return None if row is None else row[column]

    async def fetch_all(
        self,
        stmt: str,
        params: t.Optional[t.Mapping] = None,
    ) -> t.List[t.Mapping]:
        raise NotImplementedError()

    async def iterate(
        self,
        stmt: str,
        params: t.Optional[t.Mapping] = None,
    ) -> t.AsyncGenerator[t.Mapping, None]:
        raise NotImplementedError()
        # noinspection PyUnreachableCode
        yield True  # pragma: nocover

    def transaction(self) -> BaseTransaction:
        raise NotImplementedError()

    @property
    def raw_connection(self) -> t.Any:
        raise NotImplementedError()


class BaseSavePoint:
    async def begin(self) -> BaseSavePoint:
        raise NotImplementedError()

    async def commit(self):
        raise NotImplementedError()

    async def rollback(self):
        raise NotImplementedError()


class BaseTransaction:
    async def begin(self, is_root: bool = True) -> BaseTransaction:
        raise NotImplementedError()

    async def commit(self):
        raise NotImplementedError()

    async def rollback(self):
        raise NotImplementedError()


class BaseDriver:
    dialect: str = "unknown"
    can_create_database: bool = True

    async def connect(self):
        raise NotImplementedError()

    async def disconnect(self):
        raise NotImplementedError()

    def connection(self) -> BaseConnection:
        raise NotImplementedError()
