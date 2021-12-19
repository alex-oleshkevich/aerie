import typing as t

from sqlalchemy import text
from sqlalchemy.engine import Result
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.sql import Executable

from aerie.collections import Collection
from aerie.utils import convert_exceptions

T = t.TypeVar('T')


class ResultProxy(t.Generic[T]):
    def __init__(self, engine: AsyncEngine, stmt: t.Union[str, Executable], params: t.Mapping = None) -> None:
        self._text_query = isinstance(stmt, str)
        self._engine = engine
        self._stmt = text(stmt) if self._text_query else stmt
        self._params = params

    async def all(self) -> Collection[T]:
        result = await self._execute()
        return Collection(result.all())

    async def one(self) -> T:
        with convert_exceptions():
            result = await self._execute()
            return result.one()

    async def one_or_none(self) -> t.Optional[T]:
        with convert_exceptions():
            result = await self._execute()
            return result.one_or_none()

    async def first(self) -> t.Optional[T]:
        result = await self._execute()
        return result.first()

    async def scalar(self) -> t.Any:
        with convert_exceptions():
            result = await self._execute()
            return result.scalar()

    async def scalars(self) -> Collection[t.Any]:
        result = await self._execute()
        return Collection(result.scalars())

    async def scalar_one_or_none(self) -> t.Optional[t.Any]:
        with convert_exceptions():
            result = await self._execute()
            return result.scalar_one_or_none()

    async def unique(self, strategy: t.Callable = None) -> t.Optional[t.Any]:
        result = await self._execute()
        return result.unique(strategy)

    async def partitions(self, size: int = None) -> t.Iterator[t.List[t.Any]]:
        result = await self._execute()
        return result.partitions(size)

    async def _execute(self) -> Result:
        async with self._engine.begin() as connection:
            return await connection.execute(self._stmt, self._params)

    def __await__(self) -> t.Generator[Result, None, None]:
        return self._execute().__await__()
