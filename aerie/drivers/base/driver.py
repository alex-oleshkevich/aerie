from __future__ import annotations

import typing as t

import pypika as pk
from pypika import functions as fn

from aerie.drivers.base.connection import BaseConnection
from aerie.drivers.base.grammar import BaseGrammar
from aerie.terms import OnConflict, Raw

Q = t.TypeVar("Q", bound=pk.queries.Query)
QB = t.TypeVar("QB", bound=pk.queries.QueryBuilder)
IterableValues = t.Union[
    t.List[t.Mapping],
    t.Tuple[t.Mapping],
]


class BaseDriver(t.Generic[Q, QB]):
    dialect: str = "unknown"
    query_class: t.Type[QB] = pk.queries.Query
    grammar_class: t.Type[BaseGrammar] = BaseGrammar

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

    def get_grammar(self) -> BaseGrammar:
        return self.grammar_class()
