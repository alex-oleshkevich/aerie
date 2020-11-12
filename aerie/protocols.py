from __future__ import annotations

import typing as t


class SQLBuilder(t.Protocol):
    def get_sql(self) -> str: ...


Queryable = t.Union[str, SQLBuilder]


class Connection(t.Protocol):
    async def execute(self, stmt: str, params: t.Mapping = None) -> t.Any: ...

    async def execute_many(
            self, stmt: str, params: t.List[t.Mapping] = None,
    ) -> t.Any: ...

    async def fetch_one(
            self, stmt: str, params: t.Mapping = None,
    ) -> t.Mapping: ...

    async def fetch_all(
            self, stmt: str, params: t.Mapping = None,
    ) -> t.List[t.Mapping]: ...

    async def fetch_val(self, stmt: str,
                        params: t.Mapping = None) -> t.Any: ...

    async def iterate(
            self, stmt: str, params: t.Mapping = None,
    ) -> t.AsyncGenerator[t.Mapping, None]: ...

    def transaction(self) -> Transaction: ...


Row = t.Mapping


class Driver(t.Protocol):
    async def connect(self) -> t.AsyncContextManager[Connection]: ...


class SavePoint(t.Protocol):
    async def begin(self) -> SavePoint: ...

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...

    async def __aenter__(self) -> SavePoint: ...

    async def __aexit__(self, *args) -> None: ...


class Transaction(t.Protocol):
    async def begin(self) -> Transaction: ...

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...

    async def savepoint(self, name: str = None) -> SavePoint: ...

    async def __aenter__(self) -> Transaction: ...

    async def __aexit__(self, *args) -> None: ...


class BaseConnection:
    async def execute(self, stmt: str, params: t.Mapping = None) -> t.Any:
        raise NotImplementedError()

    async def execute_many(
            self, stmt: str, params: t.List[t.Mapping] = None,
    ) -> t.Any:
        raise NotImplementedError()

    async def fetch_one(
            self, stmt: str, params: t.Mapping = None,
    ) -> t.Mapping:
        raise NotImplementedError()

    async def fetch_all(
            self, stmt: str, params: t.Mapping = None,
    ) -> t.List[t.Mapping]:
        raise NotImplementedError()

    async def fetch_val(self, stmt: str, params: t.Mapping = None) -> t.Any:
        raise NotImplementedError()

    async def iterate(
            self, stmt: str, params: t.Mapping = None,
    ) -> t.AsyncGenerator[t.Mapping, None]:
        raise NotImplementedError()

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

    async def __aenter__(self) -> SavePoint:
        return await self.begin()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            await self.rollback()
        else:
            await self.commit()


class BaseTransaction:
    async def begin(self) -> Transaction:
        raise NotImplementedError()

    async def commit(self):
        raise NotImplementedError()

    async def rollback(self):
        raise NotImplementedError()

    def savepoint(self, name: str = None) -> SavePoint:
        raise NotImplementedError()

    async def __aenter__(self) -> Transaction:
        return await self.begin()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            await self.rollback()
        else:
            await self.commit()
