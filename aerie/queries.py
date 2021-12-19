from __future__ import annotations

import sys
import typing as t

from sqlalchemy import Boolean, Column, exists, func, Table
from sqlalchemy.engine import Result, Row
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import InstrumentedAttribute, joinedload, selectinload
from sqlalchemy.sql import Select, Selectable
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.sqltypes import NullType

from aerie.collections import Collection
from aerie.models import Model
from aerie.paginator import Page
from aerie.utils import colorize, convert_exceptions

M = t.TypeVar('M', Model, Table, Row)
E = t.TypeVar('E', Model, Table)


class SelectQuery(t.Generic[M]):
    def __init__(
        self, model: t.Type[M], executor: t.Union[AsyncEngine, AsyncSession], base_stmt: Select = None,
        returns_model: bool = None,
    ) -> None:
        self._model = model
        self._executor = executor
        self._stmt: Select = select(model) if base_stmt is None else base_stmt
        self._is_session = isinstance(executor, AsyncSession)
        self._returns_model = self._is_session if returns_model is None else returns_model

    def where(self, *conditions: ColumnElement[Boolean]) -> SelectQuery[M]:
        return self._clone(base_stmt=self._stmt.where(*conditions))

    def having(self, conditions: ColumnElement[Boolean]) -> SelectQuery[M]:
        return self._clone(base_stmt=self._stmt.having(conditions))

    def join(self, target: t.Union[t.Type[E], Table], *props: t.Any, full: bool = False) -> SelectQuery[M]:
        return self._clone(base_stmt=self._stmt.join(target, *props, full=full))

    def left_join(self, target: t.Union[t.Type[E], Table], on_clause: t.Any = None) -> SelectQuery[M]:
        return self._clone(base_stmt=self._stmt.outerjoin(target, on_clause))

    def filter_by(self, **kwargs: t.Any) -> SelectQuery[M]:
        return self._clone(base_stmt=self._stmt.filter_by(**kwargs))

    def group_by(self, *clauses: Column) -> SelectQuery[M]:
        return self._clone(base_stmt=self._stmt.group_by(*clauses))

    def order_by(self, *clauses: ColumnElement[NullType]) -> SelectQuery[M]:
        return self._clone(base_stmt=self._stmt.order_by(*clauses))

    def limit(self, limit: int, offset: int = None) -> SelectQuery[M]:
        stmt = self._clone(base_stmt=self._stmt.limit(limit))
        if offset:
            stmt = stmt.offset(offset)
        return stmt

    def offset(self, offset: int) -> SelectQuery[M]:
        return self._clone(base_stmt=self._stmt.offset(offset))

    def slice(self, start: int, stop: int) -> SelectQuery[M]:
        return self._clone(base_stmt=self._stmt.slice(start, stop))

    def for_update(self) -> SelectQuery[M]:
        return self._clone(base_stmt=self._stmt.with_for_update(nowait=True))

    def only(self, *cols: Column) -> SelectQuery[Row]:
        return self._clone(base_stmt=self._stmt.with_only_columns(*cols), returns_model=False)

    # def cte(self, name, recursive, nesting) -> SelectQuery:
    #     return self._clone(base_stmt=self._stmt.cte(name, recursive, nesting))

    # def subquery(self, name: str) -> SelectQuery:
    #     return self._clone(base_stmt=self._stmt.subquery(name))

    # def scalar_subquery(self) -> SelectQuery:
    #     return self._clone(base_stmt=self._stmt.scalar_subquery())

    def options(self, *options: t.Any) -> SelectQuery[M]:
        return self._clone(base_stmt=self._stmt.options(*options))

    def preload(self, *cols: InstrumentedAttribute) -> SelectQuery[M]:
        return self.options(*[joinedload(col) for col in cols])

    def prefetch(self, *cols: InstrumentedAttribute) -> SelectQuery[M]:
        return self.options(*[selectinload(col) for col in cols])

    def params(self, *args: t.Any, **kwargs: t.Any) -> SelectQuery[M]:
        """Add values for bind parameters which may have been specified in filter()."""
        return self._clone(base_stmt=self._stmt.params(*args, **kwargs))

    # def difference(self, other: SelectQuery) -> SelectQuery:
    #     self._stmt = self._stmt.except_(other)
    #     return self

    # def difference_all(self, other: SelectQuery):
    #     self._stmt = self._stmt.except_all(other)
    #     return self

    # def intersect(self, other: SelectQuery) -> SelectQuery:
    #     self._stmt = self._stmt.intersect(other)
    #     return self

    # def intersect_all(self, other: SelectQuery) -> SelectQuery:
    #     self._stmt = self._stmt.intersect_all(other)
    #     return self

    # def union(self, *q: SelectQuery) -> SelectQuery:
    #     self._stmt = self._stmt.union(*q)
    #     return self

    # def union_all(self, *q: SelectQuery) -> SelectQuery:
    #     self._stmt = self._stmt.union_all(*q)
    #     return self

    def dump(self, writer: t.IO[str] = sys.stdout, colorize_sql: bool = True) -> SelectQuery[M]:
        sql = str(self)
        if colorize_sql:
            sql = colorize(sql)
        writer.write(sql)
        return self

    def to_string(self) -> str:
        dialect = self._executor.bind.dialect if self._is_session else self._executor.dialect
        return str(self._stmt.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))

    async def one(self) -> M:
        with convert_exceptions():
            result = await self._execute(self._stmt)
            return result.scalars().one() if self._returns_model else result.one()

    async def one_or_none(self) -> t.Optional[M]:
        with convert_exceptions():
            result = await self._execute(self._stmt)
            return result.scalars().one_or_none() if self._returns_model else result.one_or_none()

    async def first(self) -> t.Optional[M]:
        result = await self._execute(self._stmt.limit(1))
        return result.scalars().first() if self._returns_model else result.first()

    async def all(self) -> Collection[M]:
        result = await self._execute(self._stmt)
        return Collection(result.scalars().all() if self._returns_model else result.all())

    async def exists(self, params: t.Mapping = None) -> bool:
        stmt = select(exists(self._stmt))
        result = await self._execute(stmt, params)
        return result.scalar() is True

    async def count(self, params: t.Mapping = None) -> int:
        stmt = select(func.count('*')).select_from(self._stmt)
        result = await self._execute(stmt, params)
        count = result.scalar()
        return int(count) if count else 0

    async def paginate(self, page: int = 1, page_size: int = 50, params: t.Mapping = None) -> Page:
        offset = (page - 1) * page_size
        total = await self.count(params)
        rows = await self.limit(page_size).offset(offset).all()
        return Page(list(rows), total, page, page_size)

    async def execute(self) -> Result:
        return await self._execute(self._stmt)

    async def _execute(self, stmt: Select, params: t.Mapping = None) -> Result:
        if isinstance(self._executor, AsyncSession):
            return await self._executor.execute(stmt, params)

        async with self._executor.begin() as connection:
            return await connection.execute(stmt, params)

    def _clone(self, *, base_stmt: Select, returns_model: bool = None) -> SelectQuery:
        return SelectQuery(
            selectable=self._model,
            executor=self._executor,
            base_stmt=base_stmt,
            returns_model=returns_model,
        )

    def __str__(self) -> str:
        return self.to_string()
