"""Write helpers, at the level the query layer does not reach."""

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from aerie import writes
from tests.models import Counter, Post, Tag


class TestColumnNames:
    def test_maps_attribute_names_to_column_names(self) -> None:
        names = writes.column_names(Tag)

        assert names["slug"] == "tag_slug"
        assert names["label"] == "tag_label"

    def test_identical_when_nothing_was_renamed(self) -> None:
        assert writes.column_names(Post)["title"] == "title"


class TestOnUpdateValues:
    def test_resolves_a_callable_default(self) -> None:
        resolved = writes.onupdate_values(Post, skip=set())

        assert "updated_at" in resolved

    def test_resolves_a_scalar_default(self) -> None:
        resolved = writes.onupdate_values(Counter, skip=set())

        assert resolved["revision"] == 99

    def test_skips_the_named_columns(self) -> None:
        assert writes.onupdate_values(Counter, skip={"revision"}) == {}


class TestScalarOnUpdateThroughUpsert:
    async def test_scalar_onupdate_is_applied_on_conflict(self, dbsession: AsyncSession) -> None:
        await writes.insert(dbsession, Counter, {"slug": "c1", "label": "first"})
        await dbsession.flush()

        await writes.upsert(dbsession, Counter, {"slug": "c1", "label": "second"}, conflict_target=["slug"])

        revision = await dbsession.scalar(sa.select(Counter.revision).where(Counter.slug == "c1"))
        assert revision == 99


class TestWriteGuards:
    """Branches the query layer never reaches, so they need direct cover."""

    async def test_empty_values_is_a_no_op(self, dbsession: AsyncSession) -> None:
        assert await writes.insert_many(dbsession, Tag, []) == []

    async def test_upsert_many_with_no_values(self, dbsession: AsyncSession) -> None:
        assert await writes.upsert_many(dbsession, Tag, [], conflict_target=["slug"]) == []

    async def test_conflict_handling_requires_a_target(self, dbsession: AsyncSession) -> None:
        with pytest.raises(ValueError, match="requires conflict_target"):
            await writes.insert(dbsession, Tag, {"slug": "x", "label": "y"}, on_conflict="set")

    async def test_nothing_to_update_is_refused(self, dbsession: AsyncSession) -> None:
        with pytest.raises(ValueError, match="no columns to update"):
            await writes.upsert(dbsession, Tag, {"slug": "x"}, conflict_target=["slug"])


class TestUpsertReturnsWrittenValues:
    async def test_returns_written_values_for_an_already_loaded_row(self, dbsession: AsyncSession) -> None:
        inserted = await writes.insert(dbsession, Tag, {"slug": "python", "label": "original"})
        await dbsession.flush()
        loaded = await dbsession.get(Tag, inserted.id)
        assert loaded is not None and loaded.label == "original"

        returned = await writes.upsert(dbsession, Tag, {"slug": "python", "label": "changed"}, conflict_target=["slug"])

        stored = await dbsession.scalar(sa.select(Tag.label).where(Tag.slug == "python"))
        assert stored == "changed"
        assert returned.label == "changed"

    async def test_returns_written_values_for_insert(self, dbsession: AsyncSession) -> None:
        row = await writes.insert(dbsession, Tag, {"slug": "rust", "label": "systems"})
        assert row.label == "systems"


class TestUpsertRefreshesOnUpdateColumns:
    async def test_updated_at_advances_on_conflict(self, dbsession: AsyncSession) -> None:
        inserted = await writes.insert(dbsession, Tag, {"slug": "go", "label": "first"})
        await dbsession.flush()
        before = inserted.updated_at
        await writes.upsert(dbsession, Tag, {"slug": "go", "label": "second"}, conflict_target=["slug"])

        after = await dbsession.scalar(sa.select(Tag.updated_at).where(Tag.slug == "go"))
        assert after is not None
        assert after > before

    async def test_created_at_is_left_alone_on_conflict(self, dbsession: AsyncSession) -> None:
        inserted = await writes.insert(dbsession, Tag, {"slug": "zig", "label": "first"})
        await dbsession.flush()
        created_at = inserted.created_at
        await writes.upsert(dbsession, Tag, {"slug": "zig", "label": "second"}, conflict_target=["slug"])

        after = await dbsession.scalar(sa.select(Tag.created_at).where(Tag.slug == "zig"))
        assert after == created_at


class TestUpsertMapsAttributeNamesToColumns:
    async def test_upsert_accepts_attribute_names(self, dbsession: AsyncSession) -> None:
        await writes.insert(dbsession, Tag, {"slug": "ruby", "label": "first"})
        await dbsession.flush()
        returned = await writes.upsert(dbsession, Tag, {"slug": "ruby", "label": "second"}, conflict_target=["slug"])
        assert returned.label == "second"

    async def test_on_conflict_nothing_accepts_attribute_names(self, dbsession: AsyncSession) -> None:
        await writes.insert(dbsession, Tag, {"slug": "perl", "label": "first"})
        await dbsession.flush()
        skipped = await writes.insert(
            dbsession,
            Tag,
            {"slug": "perl", "label": "second"},
            on_conflict="nothing",
            conflict_target=["slug"],
        )
        assert skipped is None

    async def test_explicit_replacements_accept_attribute_names(self, dbsession: AsyncSession) -> None:
        await writes.insert(dbsession, Tag, {"slug": "lua", "label": "first"})
        await dbsession.flush()
        returned = await writes.upsert(
            dbsession,
            Tag,
            {"slug": "lua", "label": "second"},
            conflict_target=["slug"],
            replacements=["label"],
        )
        assert returned.label == "second"
