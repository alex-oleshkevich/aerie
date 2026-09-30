"""Helpers for the package's database tests."""

import sqlalchemy as sa
from sqlalchemy.orm import Mapped

from aerie.columns import IntPk
from aerie.manager import DatabaseManager
from aerie.models import Base
from tests.testing import create_tables, drop_tables, temporary_database


class Widget(Base):
    __tablename__ = "test_widgets"

    id: Mapped[IntPk]
    name: Mapped[str]


METADATA = sa.MetaData()
WIDGET_TABLE = Base.metadata.tables[Widget.__tablename__]


class TestTemporaryDatabase:
    async def test_creates_and_drops_the_given_metadata(self, database_url: str) -> None:
        metadata = sa.MetaData()
        WIDGET_TABLE.to_metadata(metadata)

        async with temporary_database(database_url, metadata) as manager, manager.session() as session:
            assert (await session.execute(sa.select(sa.func.count()).select_from(WIDGET_TABLE))).scalar() == 0

        # The table is gone once the context exits.
        async with DatabaseManager(database_url) as plain, plain.session() as session:
            exists = await session.scalar(sa.text("select to_regclass('test_widgets')"))
            assert exists is None

    async def test_without_metadata_manages_no_schema(self, database_url: str) -> None:
        async with temporary_database(database_url) as manager:
            assert manager.get_engine() is not None

    async def test_nested_sessions_are_distinct(self, database_url: str) -> None:
        async with (
            temporary_database(database_url) as manager,
            manager.session() as outer,
            manager.session() as inner,
        ):
            assert outer is not inner


class TestTableHelpers:
    async def test_create_and_drop_tables(self, database_url: str) -> None:
        metadata = sa.MetaData()
        WIDGET_TABLE.to_metadata(metadata)

        async with DatabaseManager(database_url) as manager:
            await create_tables(manager, metadata)
            async with manager.session() as session:
                assert await session.scalar(sa.text("select to_regclass('test_widgets')")) is not None

            await drop_tables(manager, metadata)
            async with manager.session() as session:
                assert await session.scalar(sa.text("select to_regclass('test_widgets')")) is None
