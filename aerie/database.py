import sqlalchemy as sa
import typing as t
from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import Executable

from aerie.models import metadata as shared_metadata
from aerie.query import ExecutableQuery
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
        metadata: MetaData = None,
    ) -> None:
        self.url = url
        self.metadata = metadata or shared_metadata
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
        """Establish a new transaction."""
        return self._engine.begin()

    def query(self, stmt: Executable, params: t.Mapping = None) -> ExecutableQuery:
        return ExecutableQuery(self._engine, stmt, params)

    async def create_tables(self, tables: t.List[t.Union[str, sa.Table]] = None) -> None:
        if tables:
            tables = [self.metadata.tables[table] if isinstance(table, str) else table for table in tables]
        async with self._engine.begin() as connection:
            await connection.run_sync(self.metadata.create_all, tables=tables)

    async def drop_tables(self, tables: t.List[t.Union[str, sa.Table]] = None) -> None:
        if tables:
            tables = [self.metadata.tables[table] if isinstance(table, str) else table for table in tables]

        async with self._engine.begin() as connection:
            await connection.run_sync(self.metadata.drop_all, tables=tables)
