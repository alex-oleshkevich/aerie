from __future__ import annotations

import typing as t


class Stringable(t.Protocol):
    def __str__(self) -> str: ...


Queryable = t.Union[str, Stringable]


class Connection(t.Protocol):
    async def acquire(self) -> Connection: ...

    async def release(self) -> None: ...

    async def execute(self, stmt: str, params: t.Mapping = None) -> t.Any: ...

    async def execute_all(
            self, stmt: str, params: t.List[t.Mapping] = None,
    ) -> t.Any: ...

    async def fetch_one(
            self, stmt: str, params: t.Mapping = None,
    ) -> t.Mapping: ...

    async def fetch_all(
            self, stmt: str, params: t.Mapping = None,
    ) -> t.List[t.Mapping]: ...

    async def fetch_val(
            self, stmt: str,
            params: t.Mapping = None,
            column: t.Any = 0,
    ) -> t.Any: ...

    async def iterate(
            self, stmt: str, params: t.Mapping = None,
    ) -> t.AsyncGenerator[t.Mapping, None]: ...

    def transaction(self) -> Transaction: ...

    async def __aenter__(self) -> Connection: ...

    async def __aexit__(self, *args) -> None: ...


Row = t.Mapping


class Driver(t.Protocol):
    async def connect(self): ...

    async def disconnect(self): ...

    def connection(self) -> Connection: ...


class SavePoint(t.Protocol):
    async def begin(self) -> SavePoint: ...

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...


class Transaction(t.Protocol):
    async def begin(self, is_root: bool = True) -> Transaction: ...

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...


class BaseConnection:
    async def execute(self, stmt: str, params: t.Mapping = None) -> t.Any:
        raise NotImplementedError()

    async def execute_all(
            self, stmt: str, params: t.List[t.Mapping] = None,
    ) -> t.Any:
        raise NotImplementedError()

    async def fetch_one(
            self, stmt: str, params: t.Mapping = None,
    ) -> t.Mapping:
        raise NotImplementedError()

    async def fetch_val(
            self, stmt: str, params: t.Mapping = None, column: t.Any = 0
    ) -> t.Mapping:
        row = await self.fetch_one(stmt, params)
        return None if row is None else row[column]

    async def fetch_all(
            self, stmt: str, params: t.Mapping = None,
    ) -> t.List[t.Mapping]:
        raise NotImplementedError()

    async def iterate(
            self, stmt: str, params: t.Mapping = None,
    ) -> t.AsyncGenerator[t.Mapping, None]:
        raise NotImplementedError()
        # noinspection PyUnreachableCode
        yield True  # pragma: nocover

    def transaction(self) -> Transaction:
        raise NotImplementedError()

    @property
    def raw_connection(self) -> t.Any:
        raise NotImplementedError()


class BaseSavePoint:
    async def begin(self) -> SavePoint:
        raise NotImplementedError()

    async def commit(self):
        raise NotImplementedError()

    async def rollback(self):
        raise NotImplementedError()


class BaseTransaction:
    async def begin(self, is_root: bool = True) -> Transaction:
        raise NotImplementedError()

    async def commit(self):
        raise NotImplementedError()

    async def rollback(self):
        raise NotImplementedError()
