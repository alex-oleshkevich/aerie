"""Decoders own execution and row-to-type conversion."""

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from aerie.decoders import RowDecoder, ScalarDecoder
from tests.models import Post


class TestScalarDecoder:
    async def test_all(self, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]) -> None:
        rows = await ScalarDecoder[str]().all(dbsession, sa.select(Post.title).order_by(Post.title))
        assert rows == ["alpha-post", "beta-post", "gamma-post"]

    async def test_first(self, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]) -> None:
        assert await ScalarDecoder[str]().first(dbsession, sa.select(Post.title).order_by(Post.title)) == "alpha-post"

    async def test_one(self, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]) -> None:
        stmt = sa.select(Post.title).where(Post.title == "beta-post")
        assert await ScalarDecoder[str]().one(dbsession, stmt) == "beta-post"

    async def test_one_or_none(self, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]) -> None:
        stmt = sa.select(Post.title).where(Post.title == "nope")
        assert await ScalarDecoder[str]().one_or_none(dbsession, stmt) is None

    async def test_partitions(self, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]) -> None:
        decoder = ScalarDecoder[str]()
        batches = [list(p) async for p in decoder.partitions(dbsession, sa.select(Post.title).order_by(Post.title), 2)]
        assert batches == [["alpha-post", "beta-post"], ["gamma-post"]]


class TestScalarDecoderUnique:
    """`unique` exists for joined collection loads; it must dedupe when set."""

    async def test_unique_deduplicates(self, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]) -> None:
        stmt = sa.select(Post.blog_id).order_by(Post.blog_id)

        assert await ScalarDecoder[int]().all(dbsession, stmt) == [1, 1, 2]
        assert await ScalarDecoder[int](unique=True).all(dbsession, stmt) == [1, 2]

    async def test_unique_first(self, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]) -> None:
        stmt = sa.select(Post.blog_id).order_by(Post.blog_id)
        assert await ScalarDecoder[int](unique=True).first(dbsession, stmt) == 1

    async def test_unique_one(self, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]) -> None:
        stmt = sa.select(Post.blog_id).where(Post.blog_id == 1)
        assert await ScalarDecoder[int](unique=True).one(dbsession, stmt) == 1

    async def test_unique_one_or_none(self, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]) -> None:
        stmt = sa.select(Post.blog_id).where(Post.blog_id == 2)
        assert await ScalarDecoder[int](unique=True).one_or_none(session=dbsession, statement=stmt) == 2


class TestEntityDecoding:
    async def test_returns_entities(self, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]) -> None:
        rows = await ScalarDecoder[Post]().all(dbsession, sa.select(Post).order_by(Post.title))
        assert [row.title for row in rows] == ["alpha-post", "beta-post", "gamma-post"]
        assert isinstance(rows[0], Post)


class TestRowDecoder:
    def statement(self) -> sa.Select[int, str]:
        return sa.select(Post.blog_id, Post.title).order_by(Post.title)

    async def test_all(self, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]) -> None:
        rows = await RowDecoder[tuple[int, str]](assemble=tuple).all(dbsession, self.statement())
        assert rows == [(1, "alpha-post"), (1, "beta-post"), (2, "gamma-post")]

    async def test_first(self, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]) -> None:
        assert await RowDecoder[tuple[int, str]](assemble=tuple).first(dbsession, self.statement()) == (1, "alpha-post")

    async def test_one(self, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]) -> None:
        stmt = sa.select(Post.blog_id, Post.title).where(Post.title == "gamma-post")
        assert await RowDecoder[tuple[int, str]](assemble=tuple).one(dbsession, stmt) == (2, "gamma-post")

    async def test_one_or_none(self, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]) -> None:
        stmt = sa.select(Post.blog_id, Post.title).where(Post.title == "nope")
        assert await RowDecoder[tuple[int, str]](assemble=tuple).one_or_none(dbsession, stmt) is None

    async def test_partitions(self, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]) -> None:
        decoder = RowDecoder[tuple[int, str]](assemble=tuple)
        batches = [list(p) async for p in decoder.partitions(dbsession, self.statement(), 2)]
        assert batches == [[(1, "alpha-post"), (1, "beta-post")], [(2, "gamma-post")]]
