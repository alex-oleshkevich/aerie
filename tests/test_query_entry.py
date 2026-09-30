"""The entry point, and the writes SQLAlchemy's session does not provide.

The unit-of-work operations that used to live on a wrapper are gone: `add`, `flush`,
`commit`, `begin` and friends are `AsyncSession`'s, and testing them here would be
testing SQLAlchemy.
"""

import typing

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from aerie import writes
from aerie.querying import EntityQuery, Query, query
from tests.models import Post, Tag


class TestEntryPoint:
    def test_rejects_an_unsupported_target(self) -> None:
        with pytest.raises(TypeError, match="expects an ORM model"):
            query(typing.cast(typing.Any, object()))

    def test_opens_a_query_bound_to_the_model(self, dbsession: AsyncSession) -> None:
        posts = query(Post)

        assert isinstance(posts, EntityQuery)
        assert posts.model is Post
        assert "FROM test_posts" in str(posts.statement)

    def test_each_call_is_a_fresh_query(self, dbsession: AsyncSession) -> None:
        assert query(Post) is not query(Post)

    async def test_reads_through_the_entry_point(
        self, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        titles = await query(Post).order_by(Post.title).select(Post.title).all(dbsession)

        assert titles == ["alpha-post", "beta-post", "gamma-post"]

    async def test_opens_a_model_free_select(
        self, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        titles = await query(sa.select(Post.title).order_by(Post.title)).all(dbsession)

        assert isinstance(query(sa.select(Post.title)), Query)
        assert titles == ["alpha-post", "beta-post", "gamma-post"]

    async def test_executes_model_free_dml(self, dbsession: AsyncSession) -> None:
        inserted = await query(sa.insert(Tag).values(slug="raw", label="Raw")).execute(dbsession)
        assert inserted is not None
        assert await query(sa.select(Tag.label).where(Tag.slug == "raw")).one(dbsession) == "Raw"

        ids = await query(sa.insert(Tag).values(slug="returned", label="Returned").returning(Tag.id)).all(dbsession)
        assert len(ids) == 1

        updated = await query(sa.update(Tag).where(Tag.slug == "raw").values(label="Changed")).execute(dbsession)
        assert updated is not None
        assert await query(sa.select(Tag.label).where(Tag.slug == "raw")).one(dbsession) == "Changed"

        deleted = await query(sa.delete(Tag).where(Tag.slug == "raw")).execute(dbsession)
        assert deleted is not None
        assert await query(sa.select(Tag.id).where(Tag.slug == "raw")).one_or_none(dbsession) is None


class TestWrites:
    async def test_insert(self, dbsession: AsyncSession) -> None:
        tag = await writes.insert(dbsession, Tag, {"slug": "py", "label": "Python"})

        assert tag.label == "Python"

    async def test_insert_many(self, dbsession: AsyncSession) -> None:
        tags = await writes.insert_many(dbsession, Tag, [{"slug": "a", "label": "A"}, {"slug": "b", "label": "B"}])

        assert {tag.slug for tag in tags} == {"a", "b"}

    async def test_insert_on_conflict_nothing(self, dbsession: AsyncSession) -> None:
        await writes.insert(dbsession, Tag, {"slug": "dup", "label": "first"})
        await dbsession.flush()

        skipped = await writes.insert(
            dbsession, Tag, {"slug": "dup", "label": "second"}, on_conflict="nothing", conflict_target=["slug"]
        )

        assert skipped is None

    async def test_upsert(self, dbsession: AsyncSession) -> None:
        await writes.insert(dbsession, Tag, {"slug": "up", "label": "first"})
        await dbsession.flush()

        tag = await writes.upsert(dbsession, Tag, {"slug": "up", "label": "second"}, conflict_target=["slug"])

        assert tag.label == "second"

    async def test_upsert_many(self, dbsession: AsyncSession) -> None:
        await writes.insert(dbsession, Tag, {"slug": "m1", "label": "first"})
        await dbsession.flush()

        tags = await writes.upsert_many(
            dbsession,
            Tag,
            [{"slug": "m1", "label": "changed"}, {"slug": "m2", "label": "new"}],
            conflict_target=["slug"],
        )

        assert {tag.slug: tag.label for tag in tags} == {"m1": "changed", "m2": "new"}

    async def test_explicit_replacements(self, dbsession: AsyncSession) -> None:
        await writes.insert(dbsession, Tag, {"slug": "iw", "label": "first"})
        await dbsession.flush()

        tag = await writes.upsert(
            dbsession, Tag, {"slug": "iw", "label": "second"}, conflict_target=["slug"], replacements=["label"]
        )

        assert tag.label == "second"

    async def test_writes_are_visible_through_a_query(self, dbsession: AsyncSession) -> None:
        await writes.insert(dbsession, Tag, {"slug": "visible", "label": "yes"})
        await dbsession.flush()

        assert await query(Tag).where(Tag.slug == "visible").exists(dbsession)

    def test_pytest_marker_placeholder(self) -> None:
        assert pytest is not None
