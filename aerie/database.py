import sqlalchemy as sa
import typing as t
from contextlib import contextmanager
from sqlalchemy import text
from sqlalchemy.engine import Row
from sqlalchemy.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import Executable

from aerie.exceptions import NoResultsError, TooManyResultsError
from aerie.models import metadata
from aerie.session import DbSession

_IsolationLevel = t.Literal['SERIALIZABLE', 'REPEATABLE READ', 'READ COMMITTED', 'READ UNCOMMITTED', 'AUTOCOMMIT']


def _to_executable(stmt: t.Union[str, Executable]) -> Executable:
    return text(stmt) if isinstance(stmt, str) else stmt


class Aerie:
    def __init__(
        self,
        url: str,
        echo: bool = False,
        isolation_level: _IsolationLevel = None,
        json_serializer: t.Callable = None,
        json_deserializer: t.Callable = None,
        pool_size: int = 5,
        pool_timeout: int = 30,
        max_overflow: int = 10,
    ) -> None:
        self.url = url
        self.metadata = sa.MetaData()
        self._engine = create_async_engine(
            url,
            echo=echo,
            isolation_level=isolation_level,
            json_serializer=json_serializer,
            json_deserializer=json_deserializer,
        )
        self._session_maker = sessionmaker(bind=self._engine, class_=DbSession, expire_on_commit=False)

    def session(self) -> DbSession:
        """Create a new session object."""
        return self._session_maker()

    def transaction(self) -> AsyncEngine._trans_ctx:
        """Establish a new transction."""
        return self._engine.begin()

    async def fetch_all(self, stmt: t.Union[str, Executable], params: t.Mapping = None) -> t.List[Row]:
        async with self._engine.begin() as connection:
            stmt = text(stmt) if isinstance(stmt, str) else stmt
            result = await connection.execute(stmt, params)
            return result.all()

    async def fetch_one(self, stmt: t.Union[str, Executable], params: t.Mapping = None) -> Row:
        with self._convert_exceptions():
            async with self._engine.begin() as connection:
                stmt = _to_executable(stmt)
                result = await connection.execute(stmt, params)
                return result.one()

    async def fetch_one_or_none(self, stmt: t.Union[str, Executable], params: t.Mapping = None) -> t.Optional[Row]:
        with self._convert_exceptions():
            async with self._engine.begin() as connection:
                stmt = _to_executable(stmt)
                result = await connection.execute(stmt, params)
                return result.one_or_none()

    async def fetch_scalars(
        self, stmt: t.Union[str, Executable], params: t.Mapping = None, index: int = 0
    ) -> t.List[t.Any]:
        async with self._engine.begin() as connection:
            stmt = _to_executable(stmt)
            result = await connection.execute(stmt, params)
            return result.scalars(index=index).all()

    async def fetch_scalar(self, stmt: t.Union[str, Executable], params: t.Mapping = None) -> t.Any:
        with self._convert_exceptions():
            async with self._engine.begin() as connection:
                stmt = _to_executable(stmt)
                result = await connection.execute(stmt, params)
                return result.scalar_one()

    async def fetch_scalar_or_none(self, stmt: t.Union[str, Executable], params: t.Mapping = None) -> t.Optional[t.Any]:
        with self._convert_exceptions():
            async with self._engine.begin() as connection:
                stmt = _to_executable(stmt)
                result = await connection.execute(stmt, params)
                return result.scalar_one_or_none()

    async def execute(self, stmt: t.Union[str, Executable], params: t.Mapping = None) -> None:
        async with self._engine.begin() as connection:
            stmt = _to_executable(stmt)
            await connection.execute(stmt, params)

    async def create_tables(self, tables: t.List[t.Union[str, sa.Table]] = None) -> None:
        if tables:
            tables = [metadata.tables[table] if isinstance(table, str) else table for table in tables]
        async with self._engine.begin() as connection:
            await connection.run_sync(metadata.create_all, tables=tables)

    async def drop_tables(self, tables: t.List[t.Union[str, sa.Table]] = None) -> None:
        if tables:
            tables = [metadata.tables[table] if isinstance(table, str) else table for table in tables]

        async with self._engine.begin() as connection:
            await connection.run_sync(metadata.drop_all, tables=tables)

    @contextmanager
    def _convert_exceptions(self) -> t.Generator[None, None, None]:
        try:
            yield
        except MultipleResultsFound as exc:
            raise TooManyResultsError from exc
        except NoResultFound as exc:
            raise NoResultsError from exc
