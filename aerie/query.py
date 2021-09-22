import typing as t
from sqlalchemy import exists, func, select
from sqlalchemy.engine import Result, Row
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.sql import Executable

from aerie.utils import convert_exceptions

R = t.TypeVar('R')


class ExecutableQuery:
    def __init__(self, engine: AsyncEngine, stmt: Executable, params: t.Mapping = None) -> None:
        self._engine = engine
        self._stmt = stmt
        self._params = params

    async def first(self) -> t.Optional[Row]:
        async with self._engine.begin() as connection:
            result = await connection.execute(self._stmt, self._params)
            return result.first()

    async def all(self) -> t.List[Row]:
        async with self._engine.begin() as connection:
            result = await connection.execute(self._stmt, self._params)
            return result.all()

    async def one(self) -> Row:
        with convert_exceptions():
            async with self._engine.begin() as connection:
                result = await connection.execute(self._stmt, self._params)
                return result.one()

    async def one_or_none(self) -> t.Optional[Row]:
        with convert_exceptions():
            async with self._engine.begin() as connection:
                result = await connection.execute(self._stmt, self._params)
                return result.one_or_none()

    async def scalars(self, index: int = 0) -> t.List[t.Any]:
        async with self._engine.begin() as connection:
            result = await connection.execute(self._stmt, self._params)
            return result.scalars(index=index).all()

    async def scalar(self) -> t.Any:
        with convert_exceptions():
            async with self._engine.begin() as connection:
                result = await connection.execute(self._stmt, self._params)
                return result.scalar_one()

    async def scalar_or_none(self) -> t.Optional[t.Any]:
        with convert_exceptions():
            async with self._engine.begin() as connection:
                result = await connection.execute(self._stmt, self._params)
                return result.scalar_one_or_none()

    async def execute(self) -> Result:
        async with self._engine.begin() as connection:
            return await connection.execute(self._stmt, self._params)

    async def count(self) -> int:
        stmt = select(func.count('*')).select_from(self._stmt)
        async with self._engine.begin() as connection:
            result = await connection.execute(stmt, self._params)
            count = result.scalar()
        return int(count) if count else 0

    async def exists(self) -> bool:
        async with self._engine.begin() as connection:
            result = await connection.execute(select(exists(self._stmt)), self._params)
            return result.scalar() is True
