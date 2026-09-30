"""The reusable column aliases, exercised against a real PostgreSQL schema."""

import datetime
import typing

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import Range
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped

from aerie.columns import (
    AwareDateTime,
    Date,
    DeletedAt,
    Duration,
    Email,
    EmptyText,
    FalseBool,
    IPAddress,
    JSONDict,
    RandomUUIDPk,
    SearchVector,
    Slug,
    TextArray,
    TimeRange,
    Url,
    UUIDPk,
    ZeroInt,
)
from aerie.manager import DatabaseManager
from aerie.models import Base
from tests.models import Post


class Probe(Base):
    """One column per alias, so the DDL itself is the assertion."""

    __tablename__ = "test_probe_columns"

    id: Mapped[UUIDPk]
    slug: Mapped[Slug]
    email: Mapped[Email]
    url: Mapped[Url]
    hits: Mapped[ZeroInt]
    note: Mapped[EmptyText]
    active: Mapped[FalseBool]
    seen_at: Mapped[AwareDateTime]
    born: Mapped[Date]
    took: Mapped[Duration]
    meta: Mapped[JSONDict]
    tags: Mapped[TextArray]
    ip: Mapped[IPAddress]
    doc: Mapped[SearchVector]
    window: Mapped[TimeRange]
    deleted_at: Mapped[DeletedAt]


class Anonymous(Base):
    __tablename__ = "test_probe_anonymous"

    id: Mapped[RandomUUIDPk]


PROBE_TABLES = [Base.metadata.tables[Probe.__tablename__], Base.metadata.tables[Anonymous.__tablename__]]


@pytest.fixture
async def probe_session(database_url: str) -> typing.AsyncIterator[AsyncSession]:
    async with DatabaseManager(database_url) as manager:
        async with manager.get_engine().begin() as connection:
            await connection.run_sync(Base.metadata.drop_all, tables=PROBE_TABLES)
            await connection.run_sync(Base.metadata.create_all, tables=PROBE_TABLES)
        async with manager.session() as session:
            yield session
        async with manager.get_engine().begin() as connection:
            await connection.run_sync(Base.metadata.drop_all, tables=PROBE_TABLES)


def make_probe() -> Probe:
    now = datetime.datetime.now(datetime.UTC)
    return Probe(
        slug="hello-world",
        email="someone@example.com",
        url="https://example.com/x",
        seen_at=now,
        born=datetime.date(2020, 1, 1),
        took=datetime.timedelta(hours=2),
        tags=["alpha", "beta"],
        ip="10.0.0.1",
        doc="hello world",
        window=Range(now, now + datetime.timedelta(hours=1)),
    )


class TestPrimaryKeys:
    async def test_uuid_pk_is_time_ordered(self, probe_session: AsyncSession) -> None:
        first, second = make_probe(), make_probe()
        probe_session.add_all([first, second])
        await probe_session.flush()

        assert first.id.version == 7
        # uuid7 sorts by creation time, which is the whole point: inserts append
        # to the index instead of scattering across it.
        assert first.id < second.id

    async def test_random_uuid_pk_is_unpredictable(self, probe_session: AsyncSession) -> None:
        rows = [Anonymous(), Anonymous()]
        probe_session.add_all(rows)
        await probe_session.flush()

        assert all(row.id.version == 4 for row in rows)


class TestDefaults:
    async def test_zero_empty_and_false_defaults_apply(self, probe_session: AsyncSession) -> None:
        probe = make_probe()
        probe_session.add(probe)
        await probe_session.flush()

        assert probe.hits == 0
        assert probe.note == ""
        assert probe.active is False
        assert probe.meta == {}
        assert probe.deleted_at is None

    async def test_server_defaults_are_readable_sql(self) -> None:
        # `sa.false()` renders `DEFAULT false`; the old `server_default="0"` rendered
        # `DEFAULT '0'`, which is valid but noise in every migration diff.
        dialect = typing.cast(typing.Any, sa.dialects.postgresql.dialect)()
        ddl = str(sa.schema.CreateTable(Base.metadata.tables[Probe.__tablename__]).compile(dialect=dialect))
        assert "DEFAULT false" in ddl
        assert "DEFAULT 0" in ddl


class TestStorageTypes:
    async def test_round_trip(self, probe_session: AsyncSession) -> None:
        probe = make_probe()
        probe_session.add(probe)
        await probe_session.commit()

        stored = (await probe_session.scalars(sa.select(Probe))).one()

        assert stored.slug == "hello-world"
        assert stored.tags == ["alpha", "beta"]
        assert stored.took == datetime.timedelta(hours=2)
        assert stored.born == datetime.date(2020, 1, 1)
        assert stored.seen_at.tzinfo is not None
        assert stored.window.lower is not None

    async def test_text_array_is_a_native_array_not_jsonb(self, probe_session: AsyncSession) -> None:
        probe = make_probe()
        probe_session.add(probe)
        await probe_session.commit()

        # A native array answers containment directly; JSONB would not.
        found = await probe_session.scalar(
            sa.select(sa.func.count()).select_from(Probe).where(Probe.tags.contains(["alpha"]))
        )
        assert found == 1

    async def test_search_vector_is_queryable(self, probe_session: AsyncSession) -> None:
        probe = make_probe()
        probe_session.add(probe)
        await probe_session.commit()

        matched = await probe_session.scalar(
            sa.select(sa.func.count()).select_from(Probe).where(Probe.doc.op("@@")(sa.func.to_tsquery("hello")))
        )
        assert matched == 1


class TestSemanticAliasesCarryIndexes:
    def test_slug_and_deleted_at_are_indexed(self) -> None:
        indexed = {
            column.name for index in Base.metadata.tables[Probe.__tablename__].indexes for column in index.columns
        }

        assert "slug" in indexed
        assert "deleted_at" in indexed

    def test_structural_aliases_are_not_silently_indexed(self) -> None:
        indexed = {
            column.name for index in Base.metadata.tables[Post.__tablename__].indexes for column in index.columns
        }

        assert indexed == set()


class TestAutoUpdatedAt:
    async def test_updated_at_readable_after_update(
        self, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        """Reading updated_at right after a commit must not trigger a lazy refresh.

        On an AsyncSession that refresh raises MissingGreenlet, so serialising the
        instance outside a greenlet context would blow up.
        """
        alpha, _, _ = seeded_posts

        instance = await dbsession.get(Post, alpha.id)
        assert instance is not None

        instance.title = "first update"
        await dbsession.commit()
        timestamp_after_first_update = instance.updated_at

        instance.title = "second update"
        await dbsession.commit()

        assert instance.updated_at > timestamp_after_first_update


class TestAutoCreatedAt:
    async def test_created_at_set_on_insert(
        self, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        alpha, _, _ = seeded_posts

        instance = await dbsession.get(Post, alpha.id)
        assert instance is not None
        assert instance.created_at is not None

    async def test_created_at_untouched_by_update(
        self, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        alpha, _, _ = seeded_posts

        instance = await dbsession.get(Post, alpha.id)
        assert instance is not None
        created_at = instance.created_at

        instance.title = "updated"
        await dbsession.commit()

        assert instance.created_at == created_at
