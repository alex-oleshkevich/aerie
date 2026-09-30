import typing

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from aerie import writes
from aerie.decoders import ScalarDecoder
from aerie.querying import EntityQuery, Query, ScalarQuery
from tests.conftest import OpenQuery
from tests.models import Author, Post, Tag


class TestEscapeHatches:
    def test_statement_is_exposed(self, query: OpenQuery) -> None:
        assert "FROM test_posts" in str(query(Post).statement)

    def test_transform_applies_an_arbitrary_statement_change(self, query: OpenQuery) -> None:
        q = query(Post).transform(lambda stmt: stmt.where(Post.title == "alpha-post"))

        assert "WHERE" in str(q.statement)

    async def test_transform_preserves_result_shape(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        rows = await query(Post).transform(lambda stmt: stmt.where(Post.title == "alpha-post")).all()

        assert [row.title for row in rows] == ["alpha-post"]

    async def test_pipe_passes_the_query_through(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        def only_alpha(query: EntityQuery[Post]) -> EntityQuery[Post]:
            return query.where(Post.title == "alpha-post")

        assert len(await query(Post).pipe(only_alpha).all()) == 1

    async def test_pipe_forwards_arguments(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        def on_blog(query: EntityQuery[Post], blog_id: int) -> EntityQuery[Post]:
            return query.where(Post.blog_id == blog_id)

        assert len(await query(Post).pipe(on_blog, 1).all()) == 2

    async def test_pipe_may_change_result_shape(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        def titles(query: EntityQuery[Post]) -> ScalarQuery[str]:
            statement = typing.cast(typing.Any, query.statement).with_only_columns(
                Post.title, maintain_column_froms=True
            )
            return ScalarQuery(_statement=statement, decoder=ScalarDecoder[str]())

        assert sorted(await query(Post).pipe(titles).all()) == ["alpha-post", "beta-post", "gamma-post"]

    def test_options_attaches_loader_options(self, query: OpenQuery) -> None:
        # Entity queries already carry the raiseload("*") default, so this adds one.
        before = len(query(Post)._loader_options)

        q = query(Post).options(joinedload(Post.author))

        assert len(q._loader_options) == before + 1

    def test_statement_includes_loader_options(self, query: OpenQuery) -> None:
        q = query(Post).options(joinedload(Post.author))

        assert q.statement._with_options


class TestBuilders:
    async def test_where(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        assert len(await query(Post).where(Post.blog_id == 1).all()) == 2

    async def test_join_with_explicit_on(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        rows = await query(Post).join(Author, Post.author_id == Author.id).where(Author.name == "alice").all()
        assert len(rows) == 2

    async def test_join_without_on_uses_the_relationship(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        rows = await query(Post).join(Post.author).where(Author.name == "bob").all()
        assert len(rows) == 1

    async def test_left_join_with_explicit_on(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        assert len(await query(Post).left_join(Author, Post.author_id == Author.id).all()) == 3

    async def test_left_join_without_on(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        assert len(await query(Post).left_join(Post.author).all()) == 3

    def test_join_full_outer(self, query: OpenQuery) -> None:
        assert "FULL OUTER JOIN" in query(Post).join(Author, Post.author_id == Author.id, full=True).sql()

    def test_left_join_full_outer(self, query: OpenQuery) -> None:
        assert "FULL OUTER JOIN" in query(Post).left_join(Author, Post.author_id == Author.id, full=True).sql()

    async def test_order_by(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        rows = await query(Post).order_by(Post.title.desc()).all()
        assert [r.title for r in rows] == ["gamma-post", "beta-post", "alpha-post"]

    def test_group_by_and_having(self, query: OpenQuery) -> None:
        rendered = query(Post).group_by(Post.blog_id).having(sa.func.count(Post.id) > 1).sql()
        assert "GROUP BY" in rendered
        assert "HAVING" in rendered

    async def test_distinct(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        assert "DISTINCT" in query(Post).distinct().sql()

    async def test_limit_and_offset(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        rows = await query(Post).order_by(Post.title).limit(1).offset(1).all()
        assert [r.title for r in rows] == ["beta-post"]

    def test_limit_none_clears_the_bound(self, query: OpenQuery) -> None:
        assert "LIMIT" not in query(Post).limit(5).limit(None).sql()

    def test_offset_none_clears_the_bound(self, query: OpenQuery) -> None:
        assert "OFFSET" not in query(Post).offset(5).offset(None).sql()

    def test_lock(self, query: OpenQuery) -> None:
        assert "FOR UPDATE" in query(Post).lock().sql()

    def test_lock_variants(self, query: OpenQuery) -> None:
        assert "NOWAIT" in query(Post).lock(nowait=True).sql()
        assert "SKIP LOCKED" in query(Post).lock(skip_locked=True).sql()
        assert "FOR SHARE" in query(Post).lock(read=True).sql()

    def test_builders_do_not_mutate(self, query: OpenQuery) -> None:
        q = query(Post)
        q.where(Post.title == "x").limit(1)

        assert "WHERE" not in str(q.statement)
        assert "LIMIT" not in str(q.statement)

    async def test_when_applies_a_callback_only_when_true(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        called = False

        def predicate() -> sa.ColumnElement[bool]:
            nonlocal called
            called = True
            return Post.blog_id == 1

        rows = await query(Post).when(True, predicate).all()

        assert called is True
        assert len(rows) == 2

    async def test_when_skips_a_callback_when_false(self, query: OpenQuery) -> None:
        def predicate() -> sa.ColumnElement[bool]:
            raise AssertionError("predicate must not be called")

        filtered = query(Post).when(False, predicate)

        assert filtered.statement.compare(query(Post).statement)

    async def test_when_not_applies_a_callback_when_false(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        rows = await query(Post).when_not(False, lambda: Post.blog_id == 1).all()

        assert len(rows) == 2

    def test_when_not_skips_a_callback_when_true(self, query: OpenQuery) -> None:
        def predicate() -> sa.ColumnElement[bool]:
            raise AssertionError("predicate must not be called")

        filtered = query(Post).when_not(True, predicate)

        assert filtered.statement.compare(query(Post).statement)


class TestTerminals:
    async def test_all(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        assert len(await query(Post).all()) == 3

    async def test_first_returns_a_row(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        row = await query(Post).order_by(Post.title).first()
        assert row is not None and row.title == "alpha-post"

    async def test_first_returns_none_when_empty(self, query: OpenQuery) -> None:
        assert await query(Post).where(Post.title == "nope").first() is None

    async def test_first_applies_limit_one(self, query: OpenQuery) -> None:
        # Otherwise every row is fetched and all but one thrown away.
        assert "LIMIT" in query(Post).limit(1).sql()

    async def test_first_or_raise_returns_a_row(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        row = await query(Post).order_by(Post.title).first_or_raise()
        assert row.title == "alpha-post"

    async def test_first_or_raise_raises_by_default(self, query: OpenQuery) -> None:
        with pytest.raises(NoResultFound):
            await query(Post).where(Post.title == "nope").first_or_raise()

    async def test_first_or_raise_raises_the_given_exception(self, query: OpenQuery) -> None:
        with pytest.raises(ValueError, match="custom"):
            await query(Post).where(Post.title == "nope").first_or_raise(ValueError("custom"))

    async def test_one(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        row = await query(Post).where(Post.title == "beta-post").one()
        assert row.title == "beta-post"

    async def test_one_raises_on_many(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        with pytest.raises(MultipleResultsFound):
            await query(Post).one()

    async def test_one_or_none(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        assert await query(Post).where(Post.title == "nope").one_or_none() is None

    async def test_one_or_raise_returns_the_row(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        row = await query(Post).where(Post.title == "alpha-post").one_or_raise()

        assert row.title == "alpha-post"

    async def test_one_or_raise_raises_the_given_error_or_no_result(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        with pytest.raises(LookupError, match="gone"):
            await query(Post).where(Post.title == "nope").one_or_raise(exc=LookupError("gone"))
        with pytest.raises(NoResultFound):
            await query(Post).where(Post.title == "nope").one_or_raise()

    async def test_exists(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        assert await query(Post).where(Post.title == "alpha-post").exists() is True
        assert await query(Post).where(Post.title == "nope").exists() is False

    async def test_sql_with_and_without_literals(self, query: OpenQuery) -> None:
        q = query(Post).where(Post.title == "alpha-post")
        assert "'alpha-post'" in q.sql(literal=True)
        assert "'alpha-post'" not in q.sql()


class TestCount:
    async def test_counts_matching_rows(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        assert await query(Post).count() == 3
        assert await query(Post).where(Post.blog_id == 1).count() == 2

    async def test_count_respects_the_query_bound(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        # A bounded query counts at most its bound -- the paginator's unbounded total
        # is a different operation on purpose.
        assert await query(Post).limit(2).count() == 2
        assert await query(Post).offset(2).count() == 1

    async def test_count_is_zero_when_nothing_matches(self, query: OpenQuery) -> None:
        assert await query(Post).where(Post.title == "nope").count() == 0


class TestAggregates:
    async def test_unbounded_aggregates(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        assert await query(Post).sum(Post.blog_id) == 4
        assert await query(Post).min(Post.blog_id) == 1
        assert await query(Post).max(Post.blog_id) == 2
        assert float(await query(Post).avg(Post.blog_id) or 0) == pytest.approx(4 / 3)

    async def test_aggregate_returns_none_when_empty(self, query: OpenQuery) -> None:
        assert await query(Post).where(Post.title == "nope").sum(Post.blog_id) is None

    async def test_bounded_aggregate_runs_over_the_bounded_rows(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        # Both blog_1 posts sort first, so the bounded sum is 1+1, not 1+1+2.
        assert await query(Post).order_by(Post.title).limit(2).sum(Post.blog_id) == 2

    async def test_aggregates_a_computed_expression(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        # The aggregand is projected into the query, so it need not be a plain column.
        assert await query(Post).max(Post.blog_id * 10) == 20

    @pytest.mark.parametrize("projection", [False, True])
    async def test_grouped_transform_still_rejects_aggregate(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post], projection: bool
    ) -> None:
        transformed = query(Post).transform(lambda statement: statement.group_by(Post.blog_id))
        grouped = transformed.select(Post.blog_id) if projection else transformed

        with pytest.raises(ValueError, match="Aggregating a grouped query is ambiguous"):
            await grouped.sum(Post.id)

        assert await grouped.group_by(None).sum(Post.blog_id) == 4


class TestStreaming:
    async def test_batches(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        batches = [[r.title for r in b] async for b in query(Post).order_by(Post.title).batches(size=2)]
        assert batches == [["alpha-post", "beta-post"], ["gamma-post"]]

    async def test_iter(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        titles = [r.title async for r in query(Post).order_by(Post.title).iter(batch_size=2)]
        assert titles == ["alpha-post", "beta-post", "gamma-post"]

    async def test_iter_leaves_the_session_usable_after_early_exit(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        async for _ in query(Post).order_by(Post.title).iter(batch_size=1):
            break

        assert await query(Post).count() == 3


class TestKeysetTraversal:
    async def test_batches_by(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        batches = [[r.title for r in b] async for b in query(Post).batches_by(Post.id, size=2)]
        assert batches == [["alpha-post", "beta-post"], ["gamma-post"]]

    async def test_batches_by_stops_on_an_empty_result(self, query: OpenQuery) -> None:
        assert [b async for b in query(Post).batches_by(Post.id, size=2)] == []

    async def test_batches_by_discards_an_existing_order(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        batches = [[r.title for r in b] async for b in query(Post).order_by(Post.title.desc()).batches_by(Post.id)]
        assert batches == [["alpha-post", "beta-post", "gamma-post"]]

    async def test_iter_by(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        titles = [r.title async for r in query(Post).iter_by(Post.id, size=2)]
        assert titles == ["alpha-post", "beta-post", "gamma-post"]


class TestPrimaryKey:
    async def test_pk_finds_a_row(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        alpha, _, _ = seeded_posts
        row = await query(Post).pk(alpha.id).one()
        assert row.id == alpha.id

    async def test_pk_respects_the_rest_of_the_query(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        _, _, gamma = seeded_posts
        # gamma is on blog 2, so scoping to blog 1 must not find it.
        assert await query(Post).where(Post.blog_id == 1).pk(gamma.id).one_or_none() is None

    def test_pk_rejects_the_wrong_number_of_values(self, query: OpenQuery) -> None:
        with pytest.raises(ValueError, match="1 primary key column"):
            query(Post).pk(1, 2)


class TestScalarQuery:
    async def test_set_deduplicates(self, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]) -> None:
        statement = sa.select(Post).with_only_columns(Post.blog_id, maintain_column_froms=True)
        blog_ids = ScalarQuery[int](_statement=statement, decoder=ScalarDecoder[int]())

        assert await blog_ids.set(dbsession) == {1, 2}


class TestQueryConstruction:
    def test_for_model_builds_an_entity_query(self, dbsession: AsyncSession) -> None:
        posts = EntityQuery.for_model(Post)

        assert isinstance(posts, Query)
        assert posts.model is Post


class TestAggregateClauses:
    async def test_aggregate_over_a_renamed_column(self, query: OpenQuery, dbsession: AsyncSession) -> None:
        await writes.insert(dbsession, Tag, {"slug": "z", "label": "l"})
        await dbsession.flush()

        assert await query(Tag).limit(5).max(Tag.slug) == "z"

    async def test_order_by_survives_a_bounded_aggregate(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        assert await query(Post).order_by(Post.blog_id.desc()).limit(2).sum(Post.blog_id) == 3

    async def test_distinct_still_ranges_over_rows(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        assert await query(Post).distinct().sum(Post.blog_id) == 4

    async def test_grouped_aggregate_fails_cleanly(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        with pytest.raises(ValueError, match="Aggregating a grouped query is ambiguous"):
            await query(Post).group_by(Post.blog_id).sum(Post.id)

        assert await query(Post).count() == 3
