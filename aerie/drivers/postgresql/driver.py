from __future__ import annotations

import typing as t

import asyncpg
import pypika as pk

from aerie.drivers.base.driver import BaseDriver
from aerie.drivers.postgresql.connection import _Connection
from aerie.drivers.postgresql.grammar import PostgresGrammar
from aerie.terms import OnConflict
from aerie.url import URL


class PostgresDriver(
    BaseDriver[pk.dialects.PostgreSQLQuery, pk.dialects.PostgreQueryBuilder]
):
    dialect = "postgresql"
    query_class = pk.dialects.PostgreSQLQuery
    grammar_class = PostgresGrammar

    def __init__(self, url: URL) -> None:
        self.url = url
        self.pool: t.Optional[asyncpg.pool.Pool] = None

    async def connect(self) -> None:
        self.pool = asyncpg.create_pool(
            self.url.url,
            **self.url.options,
        )

    async def disconnect(self) -> None:
        assert self.pool, "Driver is not connected."
        await self.pool.close()
        self.pool = None

    def connection(self) -> _Connection:
        return _Connection(self.pool)

    def insert_query(
        self,
        table_name: str,
        values: t.Mapping,
        on_conflict: t.Union[str, t.List[str]] = OnConflict.RAISE,
        conflict_target: t.Union[str, t.List[str]] = None,
        replace_except: t.List[str] = None,
    ) -> pk.dialects.PostgreQueryBuilder:
        qb = super().insert_query(table_name, values)
        conflict_target = conflict_target or []
        replace_except = replace_except or []

        if on_conflict != OnConflict.RAISE:
            qb = qb.on_conflict(*conflict_target)

        if on_conflict == OnConflict.NOTHING:
            qb = qb.do_nothing()

        if on_conflict == OnConflict.REPLACE:
            for column, value in values.items():
                if column in replace_except:
                    continue
                qb = qb.do_update(column, value)
        return qb
