from __future__ import annotations

import typing as t

import pypika as pk

from aerie.drivers.base.driver import BaseDriver
from aerie.drivers.sqlite.connection import _Connection, _Pool
from aerie.terms import OnConflict
from aerie.url import URL


class SQLiteQuery(pk.queries.Query):
    @classmethod
    def _builder(cls, **kwargs) -> SQLiteQueryBuilder:
        return SQLiteQueryBuilder(**kwargs)


class SQLiteQueryBuilder(pk.queries.QueryBuilder):
    def __init__(self, **kwargs: str) -> None:
        super().__init__(dialect=pk.dialects.Dialects.SQLLITE, **kwargs)
        self._on_conflict = False
        self._on_conflict_fields: t.List[pk.queries.Field] = []
        self._on_conflict_do_nothing = False
        self._on_conflict_do_updates: t.List[t.Tuple] = []
        self._on_conflict_wheres = None
        self._on_conflict_do_update_wheres = None

    @pk.utils.builder
    def on_conflict(
        self,
        *target_fields: t.Union[str, pk.terms.Term],
    ) -> SQLiteQueryBuilder:
        if not self._insert_table:
            raise pk.queries.QueryException("On conflict only applies to insert query")

        self._on_conflict = True

        for target_field in target_fields:
            if isinstance(target_field, str):
                self._on_conflict_fields.append(self._conflict_field_str(target_field))
            elif isinstance(target_field, pk.terms.Term):
                self._on_conflict_fields.append(target_field)
        return self

    @pk.utils.builder
    def do_nothing(self) -> SQLiteQueryBuilder:
        if len(self._on_conflict_do_updates) > 0:
            raise pk.queries.QueryException("Can not have two conflict handlers")
        self._on_conflict_do_nothing = True
        return self

    @pk.utils.builder
    def do_update(
        self,
        update_field: t.Union[str, pk.queries.Field],
        update_value: t.Optional[t.Any] = None,
    ) -> SQLiteQueryBuilder:
        if self._on_conflict_do_nothing:
            raise pk.queries.QueryException("Can not have two conflict handlers")

        if isinstance(update_field, str):
            field = self._conflict_field_str(update_field)
        elif isinstance(update_field, pk.terms.Field):
            field = update_field
        else:
            raise pk.queries.QueryException("Unsupported update_field")

        if update_value is not None:
            self._on_conflict_do_updates.append(
                (field, pk.terms.ValueWrapper(update_value))
            )
        else:
            self._on_conflict_do_updates.append((field, None))
        return self

    def _conflict_field_str(self, term: str) -> t.Optional[pk.terms.Field]:
        if self._insert_table:
            return pk.terms.Field(term, table=self._insert_table)
        return None

    def _on_conflict_sql(self, **kwargs: t.Any) -> str:
        if not self._on_conflict_do_nothing and len(self._on_conflict_do_updates) == 0:
            if not self._on_conflict_fields:
                return ""
            raise pk.queries.QueryException("No handler defined for on conflict")

        if self._on_conflict_do_updates and not self._on_conflict_fields:
            raise pk.queries.QueryException(
                "Can not have fieldless on conflict do update"
            )

        conflict_query = " ON CONFLICT"
        if self._on_conflict_fields:
            fields = [
                f.get_sql(with_alias=True, **kwargs) for f in self._on_conflict_fields
            ]
            conflict_query += " (" + ", ".join(fields) + ")"

        if self._on_conflict_wheres:
            conflict_query += " WHERE {where}".format(
                where=self._on_conflict_wheres.get_sql(subquery=True, **kwargs)
            )

        return conflict_query

    def _on_conflict_action_sql(self, **kwargs: t.Any) -> str:
        if self._on_conflict_do_nothing:
            return " DO NOTHING"
        elif len(self._on_conflict_do_updates) > 0:
            updates = []
            for field, value in self._on_conflict_do_updates:
                if value:
                    updates.append(
                        "{field}={value}".format(
                            field=field.get_sql(**kwargs),
                            value=value.get_sql(with_namespace=True, **kwargs),
                        )
                    )
                else:
                    updates.append(
                        "{field}=EXCLUDED.{value}".format(
                            field=field.get_sql(**kwargs),
                            value=field.get_sql(**kwargs),
                        )
                    )
            action_sql = " DO UPDATE SET {updates}".format(updates=",".join(updates))

            if self._on_conflict_do_update_wheres:
                action_sql += " WHERE {where}".format(
                    where=self._on_conflict_do_update_wheres.get_sql(
                        subquery=True, with_namespace=True, **kwargs
                    )
                )
            return action_sql

        return ""

    def get_sql(
        self, with_alias: bool = False, subquery: bool = False, **kwargs: t.Any
    ) -> str:
        self._set_kwargs_defaults(kwargs)

        querystring = super().get_sql(with_alias, subquery, **kwargs)

        querystring += self._on_conflict_sql(**kwargs)
        querystring += self._on_conflict_action_sql(**kwargs)

        return querystring


class SQLiteDriver(BaseDriver[SQLiteQuery, SQLiteQueryBuilder]):
    dialect = "sqlite"
    can_create_database = False
    query_class = SQLiteQuery

    def __init__(self, url: URL) -> None:
        self.url = url
        self.pool = _Pool(url)

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        pass

    def connection(self) -> _Connection:
        return _Connection(self.pool)

    def insert_query(
        self,
        table_name: str,
        values: t.Mapping,
        on_conflict: t.Union[str, t.List[str]] = OnConflict.RAISE,
        conflict_target: t.Union[str, t.List[str]] = None,
        replace_except: t.List[str] = None,
    ) -> SQLiteQueryBuilder:
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
