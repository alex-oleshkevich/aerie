import typing as t
from sqlalchemy import exists, func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.sql import Executable

from aerie.utils import convert_exceptions

R = t.TypeVar('R', bound=t.Any)


class ExecutableQuery(t.Generic[R]):
    def __init__(self, engine: AsyncEngine, stmt: t.Union[str, Executable], params: t.Mapping = None) -> None:
        self._engine = engine
        self._stmt = text(stmt) if isinstance(stmt, str) else stmt
        self._original_stmt = stmt
        self._params = params

    async def first(self) -> t.Optional[R]:
        async with self._engine.begin() as connection:
            result = await connection.execute(self._stmt, self._params)
            return result.first()

    async def all(self) -> t.List[R]:
        async with self._engine.begin() as connection:
            result = await connection.execute(self._stmt, self._params)
            return result.all()

    async def one(self) -> R:
        with convert_exceptions():
            async with self._engine.begin() as connection:
                result = await connection.execute(self._stmt, self._params)
                return result.one()

    async def one_or_none(self) -> t.Optional[R]:
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

    async def execute(self) -> None:
        async with self._engine.begin() as connection:
            await connection.execute(self._stmt, self._params)

    async def count(self) -> int:
        stmt = self._stmt
        if isinstance(self._original_stmt, str):
            stmt = text('(%s) as subquery' % self._original_stmt)

        stmt = select(func.count('*')).select_from(stmt)
        async with self._engine.begin() as connection:
            result = await connection.execute(stmt, self._params)
            return result.scalar()

    async def exists(self) -> bool:
        stmt = self._stmt
        if isinstance(self._original_stmt, str):
            stmt = text('%s' % self._original_stmt)

        print(select(exists(stmt)))
        async with self._engine.begin() as connection:
            result = await connection.execute(select(exists(stmt)), self._params)
            return result.scalar()


# SELECT EXISTS (SELECT users.id, users.name
# FROM users
# WHERE users.id = :id_1) AS anon_1

# SELECT EXISTS (SELECT select * from users where id = 1) AS anon_1
