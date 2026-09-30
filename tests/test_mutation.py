"""UPDATE and DELETE built from a query's WHERE clause."""

import asyncio

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from aerie import writes
from aerie.manager import DatabaseManager
from aerie.mutation import DeleteQuery, MutationResult, UpdateQuery
from aerie.querying import query as open_query
from tests.conftest import OpenQuery
from tests.models import Author, Counter, Post, Tag


@pytest.fixture
async def counters(query: OpenQuery, dbsession: AsyncSession) -> tuple[Counter, Counter]:
    first = await writes.insert(dbsession, Counter, {"slug": "a", "label": "first"})
    second = await writes.insert(dbsession, Counter, {"slug": "b", "label": "second"})
    await dbsession.flush()
    return first, second


class TestUpdate:
    async def test_sets_a_column(self, query: OpenQuery, counters: tuple[Counter, Counter]) -> None:
        result = await query(Counter).where(Counter.slug == "a").update().set(Counter.label, "changed").execute()

        assert isinstance(result, MutationResult)
        assert result.rowcount == 1
        assert await query(Counter).where(Counter.slug == "a").select(Counter.label).one() == "changed"

    async def test_sets_several_columns(self, query: OpenQuery, counters: tuple[Counter, Counter]) -> None:
        await (
            query(Counter)
            .where(Counter.slug == "a")
            .update()
            .set(Counter.label, "changed")
            .set(Counter.revision, 7)
            .execute()
        )

        row = await query(Counter).where(Counter.slug == "a").select(Counter.label, Counter.revision).one()
        assert row == ("changed", 7)

    async def test_without_a_where_clause_updates_everything(
        self, query: OpenQuery, counters: tuple[Counter, Counter]
    ) -> None:
        result = await query(Counter).update().set(Counter.label, "all").execute()

        assert result.rowcount == 2

    async def test_respects_pk(self, query: OpenQuery, counters: tuple[Counter, Counter]) -> None:
        first, _ = counters

        result = await query(Counter).pk(first.id).update().set(Counter.label, "one").execute()

        assert result.rowcount == 1

    async def test_matching_nothing_reports_zero(self, query: OpenQuery, counters: tuple[Counter, Counter]) -> None:
        result = await query(Counter).where(Counter.slug == "nope").update().set(Counter.label, "x").execute()

        assert result.rowcount == 0

    async def test_requires_at_least_one_assignment(self, query: OpenQuery) -> None:
        with pytest.raises(ValueError, match="at least one set"):
            await query(Counter).update().execute()

    def test_is_immutable(self, query: OpenQuery) -> None:
        update = query(Counter).update()
        update.set(Counter.label, "x")

        assert update.assignments == ()

    def test_builds_an_update_statement(self, query: OpenQuery) -> None:
        statement = query(Counter).where(Counter.slug == "a").update().set(Counter.label, "x").statement()

        assert isinstance(statement, sa.Update)
        assert "WHERE" in str(statement)


class TestArithmetic:
    async def test_inc_adds_in_the_database(self, query: OpenQuery, counters: tuple[Counter, Counter]) -> None:
        await query(Counter).where(Counter.slug == "a").update().inc(Counter.revision, 5).execute()

        assert await query(Counter).where(Counter.slug == "a").select(Counter.revision).one() == 6

    async def test_dec_subtracts(self, query: OpenQuery, counters: tuple[Counter, Counter]) -> None:
        await query(Counter).where(Counter.slug == "a").update().dec(Counter.revision, 1).execute()

        assert await query(Counter).where(Counter.slug == "a").select(Counter.revision).one() == 0

    async def test_inc_and_dec_compose(self, query: OpenQuery, counters: tuple[Counter, Counter]) -> None:
        await (
            query(Counter)
            .where(Counter.slug == "a")
            .update()
            .inc(Counter.revision, 10)
            .set(Counter.label, "moved")
            .execute()
        )

        row = await query(Counter).where(Counter.slug == "a").select(Counter.revision, Counter.label).one()
        assert row == (11, "moved")


class TestReturning:
    async def test_returns_a_single_column(self, query: OpenQuery, counters: tuple[Counter, Counter]) -> None:
        first, _ = counters

        ids = (
            await query(Counter).where(Counter.slug == "a").update().set(Counter.label, "x").returning(Counter.id).all()
        )

        assert ids == [first.id]

    async def test_returns_several_columns(self, query: OpenQuery, counters: tuple[Counter, Counter]) -> None:
        rows = await (
            query(Counter)
            .where(Counter.slug == "a")
            .update()
            .set(Counter.label, "x")
            .returning(Counter.slug, Counter.label)
            .all()
        )

        assert rows == [("a", "x")]

    async def test_returns_entities(self, query: OpenQuery, counters: tuple[Counter, Counter]) -> None:
        rows = await query(Counter).where(Counter.slug == "a").update().set(Counter.label, "x").returning(Counter).all()

        assert [row.label for row in rows] == ["x"]

    async def test_terminals(self, query: OpenQuery, counters: tuple[Counter, Counter]) -> None:
        def pending() -> object:
            return query(Counter).where(Counter.slug == "a").update().set(Counter.label, "x").returning(Counter.slug)

        assert await pending().first() == "a"  # type: ignore[attr-defined]
        assert await pending().one() == "a"  # type: ignore[attr-defined]
        assert await pending().one_or_none() == "a"  # type: ignore[attr-defined]

    async def test_first_is_none_when_nothing_matched(
        self, query: OpenQuery, counters: tuple[Counter, Counter]
    ) -> None:
        returning = query(Counter).where(Counter.slug == "nope").update().set(Counter.label, "x").returning(Counter.id)

        assert await returning.first() is None
        assert await returning.one_or_none() is None


class TestDelete:
    async def test_deletes_matching_rows(self, query: OpenQuery, counters: tuple[Counter, Counter]) -> None:
        result = await query(Counter).where(Counter.slug == "a").delete().execute()

        assert result.rowcount == 1
        assert await query(Counter).count() == 1

    async def test_without_a_where_clause_deletes_everything(
        self, query: OpenQuery, counters: tuple[Counter, Counter]
    ) -> None:
        result = await query(Counter).delete().execute()

        assert result.rowcount == 2

    async def test_returning(self, query: OpenQuery, counters: tuple[Counter, Counter]) -> None:
        slugs = await query(Counter).where(Counter.slug == "a").delete().returning(Counter.slug).all()

        assert slugs == ["a"]

    def test_builds_a_delete_statement(self, query: OpenQuery) -> None:
        statement = query(Counter).where(Counter.slug == "a").delete().statement()

        assert isinstance(statement, sa.Delete)
        assert "WHERE" in str(statement)


class TestConstruction:
    def test_update_and_delete_carry_the_model(self, query: OpenQuery) -> None:
        assert isinstance(open_query(Tag).update(), UpdateQuery)
        assert isinstance(open_query(Tag).delete(), DeleteQuery)
        assert open_query(Post).update().model is Post
        assert open_query(Post).delete().model is Post


class TestMutationsHonourTheWholeQuery:
    def test_join_is_kept(self, query: OpenQuery) -> None:
        statement = str(
            query(Post).join(Author, Post.author_id == Author.id).where(Author.name == "alice").delete().statement()
        )

        assert "JOIN test_authors" in statement
        assert "test_posts.author_id = test_authors.id" in statement

    async def test_join_restricts_the_rows_written(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        result = await (
            query(Post).join(Author, Post.author_id == Author.id).where(Author.name == "bob").delete().execute()
        )

        assert result.rowcount == 1
        assert await query(Post).count() == 2

    async def test_limit_is_respected(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        result = await query(Post).where(Post.blog_id == 1).limit(1).delete().execute()

        assert result.rowcount == 1
        assert await query(Post).count() == 2

    async def test_update_honours_a_join(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        result = await (
            query(Post)
            .join(Author, Post.author_id == Author.id)
            .where(Author.name == "bob")
            .update()
            .set(Post.title, "renamed")
            .execute()
        )

        assert result.rowcount == 1

    async def test_a_plain_query_still_works(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        result = await query(Post).where(Post.blog_id == 1).delete().execute()

        assert result.rowcount == 2


class TestCompareAndSet:
    def test_a_plain_query_puts_its_predicate_on_the_statement(self, query: OpenQuery) -> None:
        statement = str(query(Counter).where(Counter.slug == "a").update().set(Counter.label, "x").statement())

        assert "SELECT" not in statement
        assert "test_counters.slug = " in statement

    def test_a_limited_query_still_goes_through_its_rows(self, query: OpenQuery) -> None:
        statement = str(query(Counter).where(Counter.slug == "a").limit(1).delete().statement())

        assert "IN (SELECT" in statement

    async def test_only_one_of_two_racing_writers_claims_the_row(self, database_manager: DatabaseManager) -> None:
        async with database_manager.session() as setup:
            row = await writes.insert(setup, Counter, {"slug": "race", "label": "open"})
            await setup.commit()

        def claim(label: str) -> UpdateQuery[Counter]:
            return open_query(Counter).pk(row.id).where(Counter.label == "open").update().set(Counter.label, label)

        try:
            async with database_manager.session() as first, database_manager.session() as second:
                won = await claim("first").execute(first)
                racing = asyncio.create_task(claim("second").execute(second))
                await asyncio.sleep(0.2)
                await first.commit()
                lost = await racing
                await second.commit()

            assert (won.rowcount, lost.rowcount) == (1, 0)
        finally:
            async with database_manager.session() as cleanup:
                await open_query(Counter).pk(row.id).delete().execute(cleanup)
                await cleanup.commit()
