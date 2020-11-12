import importlib
import typing as t
import urllib.parse

from aerie.expr import OnConflictHandler, OrderByArguments
from aerie.query import (DeleteQuery, InsertFromSelectQuery, InsertQuery,
                         RawQuery, SelectQuery, UpdateQuery)


class AdapterNotSupported(Exception): pass


class Transaction:
    async def begin(self):
        ...

    async def commit(self):
        ...

    async def rollback(self):
        ...

    async def __aenter__(self):
        return await self.begin()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            await self.rollback()
        else:
            await self.commit()


class Database:
    schemas = {
        'postgresql': 'aerie.adapters.postgresql.Adapter',
        'psql': 'aerie.adapters.postgresql.Adapter',
    }

    def __init__(self, url: str):
        self.url = url
        self.adapter = self._create_adapter(url)

    def insert(
            self,
            table: str,
            values: t.Dict,
            returning: t.List[str] = None,
            on_conflict: OnConflictHandler = None,
    ) -> InsertQuery:
        return InsertQuery(
            self.adapter, table, values, returning, on_conflict,
        )

    def insert_from_select(self, table: str, select: SelectQuery):
        return InsertFromSelectQuery(
            adapter=self.adapter,
            table=table,
            select=select,
        )

    def update(self, table: str, values, where=None,
               returning: t.List[str] = None):
        return UpdateQuery(
            adapter=self.adapter,
            table=table,
            values=values,
            where=where,
            returning=returning,
        )

    def delete(self, table: str, where=None):
        return DeleteQuery(
            adapter=self.adapter,
            table=table,
            where=where,
        )

    def select(
            self,
            table: str,
            columns: t.List = None,
            order_by: t.List[OrderByArguments] = None,
    ) -> SelectQuery:
        return SelectQuery(
            self.adapter,
            table=table,
            columns=columns,
            order_by=order_by,
        )

    def transaction(self) -> Transaction:
        return Transaction()

    async def raw(self, sql: str, params: t.Dict) -> RawQuery:
        return RawQuery(
            adapter=self.adapter,
            sql=sql,
            params=params,
        )

    def _create_adapter(self, url: str):
        url = urllib.parse.urlparse(url)
        scheme = url.scheme
        if scheme not in self.schemas:
            raise AdapterNotSupported(f'Adapter "{scheme}" not supported.')

        adapter_class = self.schemas[scheme]
        try:
            module_name, *rest, class_name = adapter_class.rpartition('.')
            module = importlib.import_module(module_name)
            klass = getattr(module, class_name)
            return klass.create_from_url(url)
        except ImportError as ex:
            print(ex)
            raise AdapterNotSupported(
                f'Adapter class could not be imported: %s' % adapter_class
            ) from ex
