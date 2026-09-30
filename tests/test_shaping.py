"""shape(): convert the rows selected by the caller into a result type."""

import dataclasses

from sqlalchemy.orm import selectinload

from aerie.querying import ShapeQuery
from aerie.querying import query as open_query
from tests.conftest import OpenQuery
from tests.models import Author, Post


@dataclasses.dataclass(frozen=True, slots=True)
class PostCard:
    id: int
    title: str


class TestShape:
    async def test_converts_explicit_projection(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        cards = await query(Post).select(Post.id, Post.title).shape(PostCard).order_by(Post.title).all()

        assert [(card.id, card.title) for card in cards] == [
            (seeded_posts[0].id, "alpha-post"),
            (seeded_posts[1].id, "beta-post"),
            (seeded_posts[2].id, "gamma-post"),
        ]

    async def test_preserves_query_clauses_and_projection(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        q = query(Post).where(Post.blog_id == 1).select(Post.id, Post.title).order_by(Post.title.desc())
        cards = await q.shape(PostCard).all()

        assert [card.title for card in cards] == ["beta-post", "alpha-post"]
        assert q.shape(PostCard).sql() == q.sql()

    def test_returns_a_shape_query(self, query: OpenQuery) -> None:
        q: ShapeQuery[PostCard] = open_query(Post).select(Post.id, Post.title).shape(PostCard)

        assert isinstance(q, ShapeQuery)

    async def test_supports_arbitrary_callable(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        cards = await query(Post).select(Post.id, Post.title).shape(lambda id, title: (id, title)).all()

        assert cards[0][1] == "alpha-post"

    async def test_supports_scalar_row(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        titles = await query(Post).select(Post.title).shape(lambda title: title.upper()).all()

        assert titles == ["ALPHA-POST", "BETA-POST", "GAMMA-POST"]

    async def test_supports_explicit_join(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        rows = await (
            query(Post)
            .join(Author, Post.author_id == Author.id)
            .select(Post.title, Author.name)
            .shape(lambda title, author: (title, author))
            .all()
        )

        assert rows[0][1] == "alice"

    async def test_load_then_shape(self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]) -> None:
        cards = await (
            query(Post)
            .options(selectinload(Post.author))
            .order_by(Post.title)
            .select(Post.title)
            .shape(lambda title: title)
            .all()
        )

        assert cards == ["alpha-post", "beta-post", "gamma-post"]

    async def test_shape_preserves_entity_loader_options(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        titles = await query(Post).options(selectinload(Post.author)).shape(lambda post: post.author.name).all()

        assert titles == ["alice", "alice", "bob"]

    async def test_shape_does_not_infer_or_replace_selected_columns(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        rows = await query(Post).select(Post.title, Post.id).shape(lambda title, id: (title, id)).all()

        assert rows[0][0] == "alpha-post"
