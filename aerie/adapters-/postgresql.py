from __future__ import annotations

import typing as t

from pypika.dialects import PostgreQueryBuilder

from aerie.adapters.sql import SQLAdapter
# class InsertQuery2(BaseInsertQuery):
#     query_class = pk.PostgreSQLQuery
#
#     def _handle_on_conflict(self, query, on_conflict: OnConflict):
#         query = query.on_conflict(
#             *on_conflict.target_fields,
#         )
#         if on_conflict.action == OnConflict.DO_NOTHING:
#             query = query.do_nothing()
#         if on_conflict.action == OnConflict.DO_UPDATE:
#             for column, value in on_conflict.update_values.items():
#                 query = query.do_update(column, value)
#         return query
#
#     def _handle_returning(self, query: pk.dialects.PostgreQueryBuilder,
#                           returning: t.List[str]):
#         return query.returning(*returning)
#
#
# class InsertAllQuery2(InsertQuery):
#     def __init__(
#             self,
#             adapter: SQLAdapter,
#             table: str,
#             values: t.List[t.Dict],
#             returning: t.Union[str, t.List[str]] = None,
#             on_conflict: OnConflictHandler = None,
#             batch_size: int = 1000,
#     ):
#         self.batch_size = batch_size
#         super().__init__(adapter=adapter, table=table, values=values,
#                          returning=returning, on_conflict=on_conflict)
from aerie.expr import OnConflict


class Adapter(SQLAdapter):
    name: str = 'postgresql'

    def __init__(self, url: str = None, *, db_name: str = None,
                 host: str = 'localhost',
                 port: int = 5432,
                 username: str = 'postgres', password: str = 'postgres',
                 options: t.Dict = None):
        pass

    def create_insert_query(self, table: str, ) -> PostgreQueryBuilder:
        return PostgreQueryBuilder().into(table)

    def build_on_conflict(self, query: PostgreQueryBuilder,
                          on_conflict: OnConflict):
        query = query.on_conflict(
            *[on_conflict.column, on_conflict.constraint])
        if on_conflict.action == OnConflict.DO_UPDATE:
            for column, value in on_conflict.update_values.items():
                query = query.do_update(column, value)
        if on_conflict.action == OnConflict.DO_NOTHING:
            query = query.do_nothing()
        return query

    def build_returning(self, query: PostgreQueryBuilder,
                        returning: t.List[str]):
        return query.returning(*returning)

# def insert(
#         self,
#         table: str,
#         values: t.Dict,
#         returning: t.Union[str, t.List[str]] = None,
#         on_conflict: OnConflictHandler = None,
# ) -> InsertQuery:
#     return InsertQuery(
#         self, table, values, returning, on_conflict,
#     )
#
# def insert_all(
#         self,
#         table: str,
#         values: t.List[t.Dict],
#         returning: t.Union[str, t.List[str]] = None,
#         on_conflict: OnConflictHandler = None,
# ) -> InsertAllQuery:
#     return InsertAllQuery(
#         adapter=self, table=table, values=values, returning=returning,
#         on_conflict=on_conflict,
#     )
