"""Relationship loading through SQLAlchemy's native ORM options."""

import typing

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, load_only, raiseload, selectinload

from aerie.decoders import ScalarDecoder
from tests.conftest import OpenQuery
from tests.models import Author, Post


class TestRaiseloadPosture:
    async def test_unloaded_relationship_raises_a_named_error(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        post = await query(Post).order_by(Post.title).first()
        assert post is not None

        with pytest.raises(InvalidRequestError, match="author"):
            _ = post.author

    async def test_preloading_makes_the_access_work(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        post = await query(Post).order_by(Post.title).options(selectinload(Post.author)).first()
        assert post is not None

        assert post.author.name == "alice"

    async def test_columns_are_unaffected(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        post = await query(Post).order_by(Post.title).first()
        assert post is not None

        assert post.title == "alpha-post"


class TestNativeOptions:
    async def test_load_only_restricts_columns(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post], dbsession: AsyncSession
    ) -> None:
        dbsession.expunge_all()

        post = await query(Post).order_by(Post.title).options(load_only(Post.id, Post.title, raiseload=True)).first()
        assert post is not None

        assert post.title == "alpha-post"
        with pytest.raises(InvalidRequestError, match="blog_id"):
            _ = post.blog_id

    async def test_nested_selectin_loaders_are_explicit(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post], dbsession: AsyncSession
    ) -> None:
        dbsession.expunge_all()

        author = await (
            query(Author)
            .where(Author.name == "alice")
            .options(
                selectinload(Author.posts).load_only(Post.id, Post.title, raiseload=True).selectinload(Post.author)
            )
            .one()
        )

        assert {post.title for post in author.posts} == {"alpha-post", "beta-post"}
        assert author.posts[0].author.name == "alice"

    async def test_joined_scalar_does_not_need_unique(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        q = query(Post).options(joinedload(Post.author))

        assert typing.cast(ScalarDecoder[typing.Any], q.decoder).unique is False
        assert len(await q.all()) == 3

    async def test_joined_collection_requires_explicit_unique(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        q = query(Author).options(joinedload(Author.posts))

        with pytest.raises(sa.exc.InvalidRequestError, match="unique"):
            await q.all()
        authors = await q.unique().all()
        assert len(authors) == 2

    async def test_unique_rejects_row_projections(self, query: OpenQuery) -> None:
        with pytest.raises(TypeError, match="scalar ORM results"):
            query(Post).select(Post.id, Post.title).unique()

    async def test_explicit_user_join_is_unchanged(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        q = query(Author).join(Post, Post.author_id == Author.id)

        assert typing.cast(ScalarDecoder[typing.Any], q.decoder).unique is False
        assert len(await q.all()) == 3

    def test_loader_options_are_visible_in_sql(self, query: OpenQuery) -> None:
        assert "JOIN" in query(Author).options(joinedload(Author.posts)).sql()
        assert "JOIN" not in query(Author).options(selectinload(Author.posts)).sql()

    def test_raw_options_can_be_combined(self, query: OpenQuery) -> None:
        statement = query(Post).options(raiseload("*"), load_only(Post.id)).statement

        assert isinstance(statement, sa.Select)


class TestUnaffectedQueries:
    async def test_projection_drops_entity_loading(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        titles = await query(Post).options(selectinload(Post.author)).order_by(Post.title).select(Post.title).all()

        assert titles == ["alpha-post", "beta-post", "gamma-post"]

    async def test_count_is_unaffected(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        assert await query(Post).options(selectinload(Post.author)).count() == 3


class TestStreamingWithRelationshipOptions:
    async def test_joined_collection_cannot_stream(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        with pytest.raises(ValueError, match="Streaming cannot deduplicate"):
            async for _ in query(Author).options(joinedload(Author.posts)).unique().batches(size=2):
                pass

    async def test_selectin_streams_fine(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        batches = [batch async for batch in query(Author).options(selectinload(Author.posts)).batches(size=2)]

        assert sum(len(batch) for batch in batches) == 2
