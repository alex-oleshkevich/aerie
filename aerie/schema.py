import sqlalchemy as sa
import typing as t
from sqlalchemy.ext.asyncio import AsyncEngine


class Schema:
    def __init__(self, engine: AsyncEngine, metadata: sa.MetaData) -> None:
        self.engine = engine
        self.metadata = metadata

    async def create_tables(self, tables: t.List[t.Union[str, sa.Table]] = None) -> None:
        if tables:
            tables = [self.metadata.tables[table] if isinstance(table, str) else table for table in tables]
        async with self.engine.begin() as connection:
            await connection.run_sync(self.metadata.create_all, tables=tables)

    async def drop_tables(self, tables: t.List[t.Union[str, sa.Table]] = None) -> None:
        if tables:
            tables = [self.metadata.tables[table] if isinstance(table, str) else table for table in tables]

        async with self.engine.begin() as connection:
            await connection.run_sync(self.metadata.drop_all, tables=tables)
