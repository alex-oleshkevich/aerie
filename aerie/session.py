from __future__ import annotations

import math
import typing as t
from sqlalchemy import exists, select
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Executable, Select
from sqlalchemy.sql.functions import func

from aerie.exceptions import TooManyResultsError
from aerie.utils import convert_exceptions

M = t.TypeVar('M')


class Page(t.Generic[M]):
    def __init__(self, rows: t.Sequence[M], total_rows: int, page: int, page_size: int) -> None:
        self.rows = rows
        self.total_rows = total_rows
        self.page = page
        self.page_size = page_size
        self._pointer = 0

    @property
    def total_pages(self) -> int:
        """Total pages in the row set."""
        return math.ceil(self.total_rows / self.page_size)

    @property
    def has_next(self) -> bool:
        """Test if the next page is available."""
        return self.page < self.total_rows

    @property
    def has_previous(self) -> bool:
        """Test if the previous page is available."""
        return self.page > 1

    @property
    def next_page(self) -> int:
        """Next page number. Always returns an integer.
        If there is no more pages the current page number returned."""
        return min(self.total_pages, self.page + 1)

    @property
    def previous_page(self) -> int:
        """Previous page number. Always returns an integer.
        If there is no previous page, the number 1 returned."""
        return max(1, self.page - 1)

    @property
    def start_index(self) -> int:
        """The 1-based index of the first item on this page."""
        if self.page == 1:
            return 1
        return (self.page - 1) * self.page_size + 1

    @property
    def end_index(self) -> int:
        """The 1-based index of the last item on this page."""
        return min(self.start_index + self.page_size - 1, self.total_rows)

    def __iter__(self) -> t.Iterator[M]:
        return iter(self.rows)

    def __next__(self) -> M:
        if self._pointer == len(self.rows):
            raise StopIteration
        self._pointer += 1
        return self.rows[self._pointer - 1]

    def __getitem__(self, item: int) -> M:
        return self.rows[item]

    def __len__(self) -> int:
        return len(self.rows)

    def __bool__(self) -> bool:
        return len(self.rows) > 0


class DbSession(AsyncSession):
    def select(self, model: t.Type[M]) -> Select:
        return select(model)

    async def all(self, stmt: Executable, params: t.Mapping = None) -> t.Sequence:
        result = await self.execute(stmt, params)
        return result.scalars().all()

    async def first(self, stmt: Executable, params: t.Mapping = None) -> t.Optional[M]:
        result = await self.execute(stmt, params)
        return result.scalars().first()

    async def one(self, stmt: Executable, params: t.Mapping = None) -> M:
        """Get exactly one result or raise.

        :raise MultipleResultsFound - when more that one row matched
        :raise NoResultFound - when no rows matched
        """

        with convert_exceptions():
            result = await self.execute(stmt, params)
            return result.scalars().one()

    async def one_or_none(self, stmt: Executable, params: t.Mapping = None) -> t.Optional[M]:
        """Get one result or return None if none found..

        :raise MultipleResultsFound"""

        try:
            result = await self.execute(stmt, params)
            return result.scalars().one_or_none()
        except MultipleResultsFound as exc:
            raise TooManyResultsError() from exc

    async def count(self, stmt: Executable, params: t.Mapping = None) -> int:
        stmt = select(func.count('*')).select_from(stmt)
        return await self.scalar(stmt, params)

    async def exists(self, stmt: Executable, params: t.Mapping = None) -> bool:
        return await self.scalar(select(exists(stmt)), params)

    async def paginate(self, stmt: Select, page: int = 1, page_size: int = 50, params: t.Mapping = None) -> Page:
        total = await self.count(stmt, params)
        offset = (page - 1) * page_size

        stmt = stmt.limit(page_size).offset(offset)
        rows = await self.all(stmt, params)
        return Page(rows, total, page, page_size)
