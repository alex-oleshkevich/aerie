from __future__ import annotations

import math
import typing as t
from sqlalchemy import exists, select
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select
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
        return self.page < self.total_pages

    @property
    def has_previous(self) -> bool:
        """Test if the previous page is available."""
        return self.page > 1

    @property
    def has_other(self) -> bool:
        """Test if page has next or previous pages."""
        return self.has_next or self.has_previous

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
        """A shortcut to check if page has more than one pages.
        Useful in templates to check if the pagination should be rendered or not."""
        return self.total_pages > 1

    def __str__(self) -> str:
        return (
            f'Page {self.page} of {self.total_pages}, rows {self.start_index} - {self.end_index} of {self.total_rows}.'
        )

    def __repr__(self) -> str:
        return f'<Page: page={self.page}, total_pages={self.total_pages}>'


class ExecutableQuery:
    def __init__(self, session: DbSession, stmt: Select, params: t.Mapping = None) -> None:
        self._session = session
        self._stmt = stmt
        self._params = params

    async def all(self) -> t.Sequence:
        result = await self._session.execute(self._stmt, self._params)
        return result.scalars().all()

    async def first(self) -> t.Optional[M]:
        result = await self._session.execute(self._stmt, self._params)
        return result.scalars().first()

    async def one(self) -> M:
        """Get exactly one result or raise.

        :raise MultipleResultsFound - when more that one row matched
        :raise NoResultFound - when no rows matched
        """

        with convert_exceptions():
            result = await self._session.execute(self._stmt, self._params)
            return result.scalars().one()

    async def one_or_none(self) -> t.Optional[M]:
        """Get one result or return None if none found..

        :raise MultipleResultsFound"""

        try:
            result = await self._session.execute(self._stmt, self._params)
            return result.scalars().one_or_none()
        except MultipleResultsFound as exc:
            raise TooManyResultsError() from exc

    async def count(self) -> int:
        stmt = select(func.count('*')).select_from(self._stmt)
        return await self._session.scalar(stmt, self._params)

    async def exists(self) -> bool:
        return await self._session.scalar(select(exists(self._stmt)), self._params)

    async def paginate(self, page: int = 1, page_size: int = 50) -> Page:
        total = await self._session.query(self._stmt, self._params).count()
        offset = (page - 1) * page_size

        stmt = self._stmt.limit(page_size).offset(offset)
        rows = await self._session.query(stmt, self._params).all()
        return Page(rows, total, page, page_size)


class DbSession(AsyncSession):
    def select(self, model: t.Type[M]) -> Select:
        return select(model)

    def query(self, stmt: Select, params: t.Mapping = None) -> ExecutableQuery:
        return ExecutableQuery(self, stmt, params)
