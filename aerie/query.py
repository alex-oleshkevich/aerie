from __future__ import annotations

import sys
import typing as t

import pypika as pk

from aerie.adapters.base import Adapter
from aerie.adapters.postgresql import Adapter
from aerie.expr import OnConflict, OnConflictHandler, OrderBy, OrderByArguments


class Query:
    def get_sql(self) -> str:
        return str(self.build_query())

    def build_query(self) -> str:
        raise NotImplementedError()

    async def execute(self):
        pass

    def dump(self, writer=sys.stdout) -> Query:
        writer.write(self.get_sql() + '\n')
        return self

    def __await__(self):
        return self.execute()

    def __str__(self):
        return self.get_sql()


class InvalidValues(Exception):
    pass


class InsertFromSelectQuery(Query):
    def __init__(self, adapter: Adapter, table: str, select: SelectQuery):
        self._adapter = adapter
        self._table = table
        self._select = select


class SelectQuery(Query):
    def __init__(self, adapter: Adapter, table: str, columns: t.List,
                 order_by: t.List[OrderByArguments] = None):
        self._adapter = adapter
        self.table = table
        self._columns = columns
        self._joins = []
        self._where = []
        self._having = []
        self._group_by = []
        self._order_by = order_by or []

    def where(self, *clauses):
        if not len(clauses):
            self._where = []
        else:
            self._where.extend(clauses)

    def having(self, *clauses):
        if not len(clauses):
            self._having = []
        else:
            self._having.extend(clauses)

    def order_by(self, *ordering: OrderByArguments):
        if not len(ordering):
            self._order_by = []
            return self

        for rule in ordering:
            if isinstance(rule, str):
                sorting = OrderBy.ASC
                column = rule
                if rule.startswith('-'):
                    sorting = OrderBy.DESC
                    column = rule[1:]
                self._order_by.append(OrderBy(column, sorting))
            elif callable(rule):
                self._order_by.append(rule(OrderBy()))
            else:
                self._order_by.append(rule)


class UpdateQuery(Query):
    def __init__(self, adapter: Adapter, table: str, values: t.Dict,
                 where=None, returning: t.List[str] = None):
        self._adapter = adapter
        self._table = table
        self._values = values
        self._where = where
        self._returning = returning


class DeleteQuery(Query):
    def __init__(self, adapter: Adapter, table: str, where=None):
        self._adapter = adapter
        self._table = table
        self._where = where


class RawQuery(Query):
    def __init__(self, adapter: Adapter, sql: str, params: t.Dict):
        self._adapter = adapter
        self._sql = sql
        self._params = params


class InsertQuery(Query):
    def __init__(
            self,
            adapter: Adapter,
            table: str,
            values: t.Union[t.Dict, t.List[t.Dict]],
            returning: t.Union[str, t.List[str]] = None,
            on_conflict: OnConflictHandler = None,
    ):
        self.adapter = adapter
        self.table = table
        self.values = values
        self.returning = (
            [returning] if isinstance(returning, str) else returning
        )

        if callable(on_conflict):
            on_conflict = on_conflict(OnConflict())
        self.on_conflict: OnConflict = on_conflict

    def build_query(self) -> pk.queries.QueryBuilder:
        query = self.adapter.create_insert_query(self.table)
        query = query.insert(self._insert_values())
        query = query.columns(self._insert_columns())
        if self.on_conflict:
            query = self.adapter.build_on_conflict(query, self.on_conflict)

        if self.returning:
            query = self.adapter.build_returning(query, self.returning)

        return query

    def _insert_columns(self):
        if isinstance(self.values, (tuple, set, list)):
            return list(self.values[0].keys())
        return list(self.values.keys())

    def _insert_values(self):
        if not isinstance(self.values, (tuple, set, list)):
            return list(self.values.values())
        return self.values


class InsertAllQuery(InsertQuery):
    pass
