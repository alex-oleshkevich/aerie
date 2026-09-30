"""Small helpers for isolated async database tests."""

import contextlib
import typing

import sqlalchemy as sa

from aerie.manager import DatabaseManager


async def create_tables(manager: DatabaseManager, metadata: sa.MetaData) -> None:
    """Create every table in ``metadata`` using the manager's engine."""
    async with manager.get_engine().begin() as connection:
        await connection.run_sync(metadata.create_all)


async def drop_tables(manager: DatabaseManager, metadata: sa.MetaData) -> None:
    """Drop every table in ``metadata`` using the manager's engine."""
    async with manager.get_engine().begin() as connection:
        await connection.run_sync(metadata.drop_all)


@contextlib.asynccontextmanager
async def temporary_database(
    url: str,
    metadata: sa.MetaData | None = None,
) -> typing.AsyncIterator[DatabaseManager]:
    """Run a database manager and optionally create and drop a test schema."""
    async with DatabaseManager(url) as manager:
        if metadata is not None:
            await create_tables(manager, metadata)
        try:
            yield manager
        finally:
            if metadata is not None:
                await drop_tables(manager, metadata)
