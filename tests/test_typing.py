"""Static typing assertions.

Everything here lives under `TYPE_CHECKING`: mypy analyses the bodies and proves the
`select()` overloads actually narrow, while nothing executes at runtime. A runtime
test cannot catch a wrong overload -- `assert_type` is a no-op once running -- so this
is the only place the shape contract is genuinely verified.

Regenerate the overloads with tools/generate_select_overloads.py.
"""

import typing

if typing.TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from aerie.querying import EntityQuery, ScalarQuery, TupleQuery, query
    from tests.models import Post
    from tests.test_query_entry import PostQuery

    def _select_narrows_the_result_type(session: AsyncSession) -> None:
        typing.assert_type(query(Post), EntityQuery[Post])
        typing.assert_type(query(Post).select(Post.id), ScalarQuery[int])
        typing.assert_type(query(Post).select(Post.id, Post.title), TupleQuery[int, str])
        typing.assert_type(query(Post).select(Post.id, Post.title, Post.blog_id), TupleQuery[int, str, int])

    async def _terminals_carry_the_declared_shape(session: AsyncSession) -> None:
        typing.assert_type(await query(Post).all(session), list[Post])
        typing.assert_type(await query(Post).first(session), Post | None)
        typing.assert_type(await query(Post).one(session), Post)

        typing.assert_type(await query(Post).select(Post.title).all(session), list[str])
        typing.assert_type(await query(Post).select(Post.title).first(session), str | None)
        typing.assert_type(await query(Post).select(Post.title).set(session), set[str])

        typing.assert_type(await query(Post).select(Post.id, Post.title).all(session), list[tuple[int, str]])
        typing.assert_type(await query(Post).select(Post.id, Post.title).first(session), tuple[int, str] | None)

    def _builders_preserve_the_shape(session: AsyncSession) -> None:
        typing.assert_type(query(Post).where(Post.blog_id == 1), EntityQuery[Post])
        typing.assert_type(query(Post).when(True, lambda: Post.blog_id == 1), EntityQuery[Post])
        typing.assert_type(query(Post).when_not(False, lambda: Post.blog_id == 1), EntityQuery[Post])
        typing.assert_type(query(Post).limit(1).offset(1).order_by(Post.title), EntityQuery[Post])
        typing.assert_type(query(Post).select(Post.title).where(Post.blog_id == 1), ScalarQuery[str])

    async def _query_classes_keep_their_type(session: AsyncSession) -> None:
        typing.assert_type(query(PostQuery), PostQuery)
        typing.assert_type(PostQuery.for_model(), PostQuery)
        typing.assert_type(query(PostQuery).in_blog(1).order_by(Post.title), PostQuery)
        typing.assert_type(await query(PostQuery).in_blog(1).one_or_none(session), Post | None)

    async def _map_overloads(session: AsyncSession) -> None:
        typing.assert_type(await query(Post).map(session, Post.id), dict[int, Post])
        typing.assert_type(await query(Post).map(session, Post.id, Post.title), dict[int, str])

    async def _aggregates_and_pipe(session: AsyncSession) -> None:
        typing.assert_type(await query(Post).count(session), int)
        typing.assert_type(await query(Post).sum(session, Post.blog_id), int | None)

        def titles(query: EntityQuery[Post]) -> ScalarQuery[str]:
            return query.select(Post.title)

        typing.assert_type(query(Post).pipe(titles), ScalarQuery[str])
