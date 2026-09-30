"""Projection: select() narrows the result shape, map() materialises a dict."""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from aerie.querying import ScalarQuery, TupleQuery
from aerie.querying import query as open_query
from tests.conftest import OpenQuery
from tests.models import Author, Post


class TestSelect:
    async def test_single_column_yields_scalars(
        self, query: OpenQuery, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        q = open_query(Post).order_by(Post.title).select(Post.title)

        assert isinstance(q, ScalarQuery)
        assert await q.all(dbsession) == ["alpha-post", "beta-post", "gamma-post"]

    async def test_multiple_columns_yield_tuples(
        self, query: OpenQuery, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        q = open_query(Post).order_by(Post.title).select(Post.blog_id, Post.title)

        assert isinstance(q, TupleQuery)
        assert await q.all(dbsession) == [(1, "alpha-post"), (1, "beta-post"), (2, "gamma-post")]

    async def test_keeps_the_filters_built_so_far(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        titles = await query(Post).where(Post.blog_id == 1).order_by(Post.title).select(Post.title).all()

        assert titles == ["alpha-post", "beta-post"]

    async def test_keeps_joins(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        names = await (
            query(Post).join(Author, Post.author_id == Author.id).where(Author.name == "bob").select(Post.title).all()
        )

        assert names == ["gamma-post"]

    async def test_can_be_narrowed_further(
        self, query: OpenQuery, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        q = open_query(Post).select(Post.blog_id, Post.title).select(Post.title)

        assert isinstance(q, ScalarQuery)
        assert sorted(await q.all(dbsession)) == ["alpha-post", "beta-post", "gamma-post"]

    async def test_terminals_work_on_a_projection(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        q = query(Post).where(Post.title == "alpha-post").select(Post.title)

        assert await q.one() == "alpha-post"
        assert await q.first() == "alpha-post"
        assert await q.count() == 1
        assert await q.exists() is True

    async def test_scalar_query_set_deduplicates(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        assert await query(Post).select(Post.blog_id).set() == {1, 2}

    async def test_tuple_projection_streams(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        q = query(Post).order_by(Post.title).select(Post.blog_id, Post.title)

        batches = [list(batch) async for batch in q.batches(size=2)]

        assert batches == [[(1, "alpha-post"), (1, "beta-post")], [(2, "gamma-post")]]

    @pytest.mark.parametrize("count", [1, 2, 3, 12])
    async def test_overload_arity(self, query: OpenQuery, count: int, seeded_posts: tuple[Post, Post, Post]) -> None:
        columns = [Post.id, Post.title, Post.blog_id, Post.author_id, Post.created_at, Post.updated_at] * 2
        rows = await query(Post).select(*columns[:count]).all()

        assert len(rows) == 3


class TestMap:
    async def test_single_column_keys_entities(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        alpha, beta, gamma = seeded_posts

        mapped = await query(Post).map(Post.id)

        assert set(mapped) == {alpha.id, beta.id, gamma.id}
        assert mapped[alpha.id].title == "alpha-post"

    async def test_two_columns_map_values(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        alpha, _, _ = seeded_posts

        mapped = await query(Post).map(Post.id, Post.title)

        assert mapped[alpha.id] == "alpha-post"
        assert len(mapped) == 3

    async def test_respects_filters(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        mapped = await query(Post).where(Post.blog_id == 2).map(Post.id, Post.title)

        assert list(mapped.values()) == ["gamma-post"]


class TestProjectionLoaderOptions:
    async def test_load_then_map(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        mapped = await query(Post).options(selectinload(Post.author)).map(Post.id, Post.title)

        assert len(mapped) == 3
