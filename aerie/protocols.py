from __future__ import annotations

import typing as t

import pypika as pk
import pypika.functions as fn

from aerie.terms import OnConflict, Raw


class StrLike(t.Protocol):
    def __str__(self) -> str:
        ...


Queryable = t.Union[str, StrLike]


class Row(t.Mapping):
    pass


class BaseConnection:
    async def acquire(self) -> None:
        raise NotImplementedError()

    async def release(self) -> None:
        raise NotImplementedError()

    async def execute(self, stmt: str, params: t.Mapping = None) -> t.Any:
        raise NotImplementedError()

    async def execute_all(
        self,
        stmt: str,
        params: t.List[t.Mapping] = None,
    ) -> t.Any:
        raise NotImplementedError()

    async def fetch_one(
        self,
        stmt: str,
        params: t.Mapping = None,
    ) -> t.Optional[t.Mapping]:
        raise NotImplementedError()

    async def fetch_val(
        self, stmt: str, params: t.Mapping = None, column: t.Any = 0
    ) -> t.Optional[t.Any]:
        row = await self.fetch_one(stmt, params)
        return None if row is None else row[column]

    async def fetch_all(
        self,
        stmt: str,
        params: t.Mapping = None,
    ) -> t.List[t.Mapping]:
        raise NotImplementedError()

    async def iterate(
        self,
        stmt: str,
        params: t.Mapping = None,
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


Q = t.TypeVar("Q", bound=pk.queries.Query)
QB = t.TypeVar("QB", bound=pk.queries.QueryBuilder)

IterableValues = t.Union[
    t.List[t.Mapping],
    t.Tuple[t.Mapping],
]


class BaseDriver(t.Generic[Q, QB]):
    dialect: str = "unknown"
    can_create_database: bool = True
    query_class: t.Type[QB] = pk.queries.Query

    async def connect(self):
        raise NotImplementedError()

    async def disconnect(self):
        raise NotImplementedError()

    def connection(self) -> BaseConnection:
        raise NotImplementedError()

    def get_query_builder(self) -> Q:
        return self.query_class()

    def count_query(
        self,
        table_name: str,
        column: str = "*",
        where: t.Union[str, pk.terms.Term] = None,
    ) -> QB:
        query = self.get_query_builder()
        query = query.from_(table_name).select(fn.Count(column))
        if where:
            if isinstance(where, str):
                query = query.where(Raw(where))
            else:
                query = query.where(where)
        return query

    def insert_query(
        self,
        table_name: str,
        values: t.Mapping,
        on_conflict: str = OnConflict.RAISE,
        conflict_target: t.Union[str, t.List[str]] = None,
        replace_except: t.List[str] = None,
    ) -> QB:
        qb = self.get_query_builder()
        columns = values.keys()
        _values = [pk.NamedParameter(column) for column in columns]
        return qb.into(table_name).columns(*columns).insert(*_values)

    def insert_all_query(
        self,
        table_name: str,
        values: IterableValues,
    ) -> QB:
        values_0 = values[0]
        return self.insert_query(table_name, values_0)

    def update_query(
        self,
        table_name: str,
        values: t.Mapping,
        where: t.Union[str, pk.terms.Term] = None,
    ) -> QB:
        qb = self.get_query_builder()
        update = qb.update(table_name)
        for key, value in values.items():
            update = update.set(key, pk.NamedParameter(key))

        if where:
            if isinstance(where, str):
                update = update.where(Raw(where))
            else:
                update = update.where(where)
        return update

    def delete_query(
        self,
        table_name: str,
        where: t.Union[str, pk.terms.Term] = None,
    ) -> QB:
        qb = self.get_query_builder()
        query = qb.from_(table_name).delete()
        if where:
            if isinstance(where, str):
                query = query.where(Raw(where))
            else:
                query = query.where(where)
        return query
