from __future__ import annotations

import sys
import typing as t
from sqlalchemy import Boolean, Column, delete, exists, func, update
from sqlalchemy.engine import Result
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import InstrumentedAttribute, joinedload, selectinload
from sqlalchemy.sql import Executable, Select
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.sqltypes import NullType

from aerie.base import Base
from aerie.collections import Collection
from aerie.paginator import Page
from aerie.utils import colorize, convert_exceptions

M = t.TypeVar('M', bound=Base)
E = t.TypeVar('E', bound=Base)


class SelectQuery(t.Generic[M]):
    def __init__(
        self,
        model: t.Type[M],
        executor: AsyncSession,
        base_stmt: Select = None,
    ) -> None:
        self._model: t.Type[Base] = model
        self._executor = executor
        self._stmt: Select = select(model) if base_stmt is None else base_stmt
        self._is_session = isinstance(executor, AsyncSession)

    def where(self, *conditions: ColumnElement[Boolean]) -> SelectQuery[M]:
        return self._clone(base_stmt=self._stmt.where(*conditions))

    def having(self, conditions: ColumnElement[Boolean]) -> SelectQuery[M]:
        return self._clone(base_stmt=self._stmt.having(conditions))

    def join(self, target: t.Type[E], on_clause: t.Any = None, full: bool = False) -> SelectQuery[M]:
        return self._clone(base_stmt=self._stmt.join(target, onclause=on_clause, full=full))

    def left_join(self, target: t.Type[E], on_clause: t.Any = None, full: bool = False) -> SelectQuery[M]:
        return self._clone(base_stmt=self._stmt.outerjoin(target, on_clause, full=full))

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
        return str(self._stmt.compile(dialect=self._executor.bind.dialect, compile_kwargs={"literal_binds": True}))

    async def one(self) -> M:
        with convert_exceptions():
            result = await self._execute(self._stmt)
            return result.scalars().one()

    async def one_or_none(self) -> t.Optional[M]:
        with convert_exceptions():
            result = await self._execute(self._stmt)
            return result.scalars().one_or_none()

    async def first(self) -> t.Optional[M]:
        result = await self._execute(self._stmt.limit(1))
        return result.scalars().first()

    async def all(self) -> Collection[M]:
        result = await self._execute(self._stmt)
        return Collection(result.scalars().all())

    async def choices(self, label_column: str = 'name', value_column: str = 'id') -> t.List[t.Tuple[str, t.Any]]:
        result = await self._execute(self._stmt)
        return Collection(result.scalars().all()).choices(label_col=label_column, value_col=value_column)

    async def choices_dict(
        self, label_column: str = 'name', value_column: str = 'id', label_key: str = 'label', value_key: str = 'value'
    ) -> list[t.Dict[t.Any, t.Any]]:
        result = await self._execute(self._stmt)
        return Collection(result.scalars().all()).choices_dict(
            label_col=label_column,
            value_col=value_column,
            label_key=label_key,
            value_key=value_key,
        )

    async def exists(self) -> bool:
        stmt = select(exists(self._stmt))
        result = await self._execute(stmt)
        return result.scalar() is True

    async def count(self) -> int:
        stmt = select(func.count('*')).select_from(self._stmt)
        result = await self._execute(stmt)
        count = result.scalar()
        return int(count) if count else 0

    async def paginate(self, page: int = 1, page_size: int = 50) -> Page:
        offset = (page - 1) * page_size
        total = await self.count()
        rows = await self.limit(page_size).offset(offset).all()
        return Page(list(rows), total, page, page_size)

    async def update(self, **values: t.Any) -> None:
        stmt = update(self._model).where(self._stmt.whereclause).values(**values)
        await self._execute(stmt)

    async def delete(self) -> None:
        stmt = delete(self._model).where(self._stmt.whereclause)
        await self._execute(stmt)

    async def execute(self) -> Result:
        return await self._execute(self._stmt)

    async def _execute(self, stmt: Executable, params: t.Mapping = None) -> Result:
        return await self._executor.execute(stmt, params)

    def _clone(self, *, base_stmt: Select) -> SelectQuery:
        return SelectQuery(
            model=self._model,
            executor=self._executor,
            base_stmt=base_stmt,
        )

    def __await__(self) -> t.Generator[t.Any, None, Collection[M]]:
        return self.all().__await__()

    def __str__(self) -> str:
        return self.to_string()
