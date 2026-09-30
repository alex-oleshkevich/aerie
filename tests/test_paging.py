"""Offset and keyset pagination."""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from aerie.paging import CursorPage, Page
from tests.conftest import OpenQuery
from tests.models import Author, Post


@pytest.fixture
async def many_posts(dbsession: AsyncSession, author: Author) -> list[Post]:
    posts = [Post(title=f"post-{index:02d}", blog_id=author.blog_id, author_id=author.id) for index in range(1, 11)]
    dbsession.add_all(posts)
    await dbsession.flush()
    return posts


class TestPage:
    def test_iterates_and_sizes(self) -> None:
        page = Page(items=["a", "b"], total=5, number=1, size=2)

        assert list(page) == ["a", "b"]
        assert len(page) == 2

    def test_page_count(self) -> None:
        assert Page[str](items=[], total=5, number=1, size=2).pages == 3
        assert Page[str](items=[], total=4, number=1, size=2).pages == 2
        assert Page[str](items=[], total=0, number=1, size=2).pages == 0

    def test_page_count_with_zero_size(self) -> None:
        assert Page[str](items=[], total=5, number=1, size=0).pages == 0

    def test_navigation_flags(self) -> None:
        first: Page[str] = Page(items=[], total=5, number=1, size=2)
        middle: Page[str] = Page(items=[], total=5, number=2, size=2)
        last: Page[str] = Page(items=[], total=5, number=3, size=2)

        assert (first.has_previous, first.has_next) == (False, True)
        assert (middle.has_previous, middle.has_next) == (True, True)
        assert (last.has_previous, last.has_next) == (True, False)


class TestPaginate:
    async def test_first_page(self, query: OpenQuery, many_posts: list[Post]) -> None:
        page = await query(Post).order_by(Post.title).paginate(number=1, size=3)

        assert [post.title for post in page] == ["post-01", "post-02", "post-03"]
        assert page.total == 10
        assert page.pages == 4

    async def test_middle_page(self, query: OpenQuery, many_posts: list[Post]) -> None:
        page = await query(Post).order_by(Post.title).paginate(number=2, size=3)

        assert [post.title for post in page] == ["post-04", "post-05", "post-06"]

    async def test_partial_last_page(self, query: OpenQuery, many_posts: list[Post]) -> None:
        page = await query(Post).order_by(Post.title).paginate(number=4, size=3)

        assert [post.title for post in page] == ["post-10"]
        assert page.has_next is False

    async def test_beyond_the_end(self, query: OpenQuery, many_posts: list[Post]) -> None:
        page = await query(Post).order_by(Post.title).paginate(number=99, size=3)

        assert list(page) == []
        assert page.total == 10

    async def test_page_zero_clamps_to_the_first(self, query: OpenQuery, many_posts: list[Post]) -> None:
        page = await query(Post).order_by(Post.title).paginate(number=0, size=3)

        assert [post.title for post in page] == ["post-01", "post-02", "post-03"]

    async def test_total_ignores_a_limit_on_the_query(self, query: OpenQuery, many_posts: list[Post]) -> None:
        # count() respects the query's own bound; a paginator's total must not.
        page = await query(Post).order_by(Post.title).limit(2).paginate(number=1, size=3)

        assert page.total == 10

    async def test_respects_filters(self, query: OpenQuery, many_posts: list[Post]) -> None:
        page = await query(Post).where(Post.title == "post-01").paginate(number=1, size=3)

        assert page.total == 1

    async def test_works_on_a_projection(self, query: OpenQuery, many_posts: list[Post]) -> None:
        page = await query(Post).order_by(Post.title).select(Post.title).paginate(number=1, size=2)

        assert list(page) == ["post-01", "post-02"]


class TestCursorPage:
    def test_iterates_and_sizes(self) -> None:
        page = CursorPage(items=["a"], next_cursor=(1,))

        assert list(page) == ["a"]
        assert len(page) == 1
        assert page.has_next is True

    def test_no_cursor_means_no_next(self) -> None:
        assert CursorPage(items=[], next_cursor=None).has_next is False

    async def test_walks_forward(self, query: OpenQuery, many_posts: list[Post]) -> None:
        seen: list[str] = []
        cursor: tuple[object, ...] | None = None

        while True:
            page = await query(Post).cursor_page(size=4, by=[Post.id], after=cursor)
            seen.extend(post.title for post in page)
            if not page.has_next:
                break
            cursor = page.next_cursor

        assert seen == [f"post-{index:02d}" for index in range(1, 11)]

    async def test_descending(self, query: OpenQuery, many_posts: list[Post]) -> None:
        page = await query(Post).cursor_page(size=3, by=[Post.id], descending=True)

        assert [post.title for post in page] == ["post-10", "post-09", "post-08"]

    async def test_multi_column_cursor(self, query: OpenQuery, many_posts: list[Post]) -> None:
        first = await query(Post).cursor_page(size=4, by=[Post.blog_id, Post.id])
        second = await query(Post).cursor_page(size=4, by=[Post.blog_id, Post.id], after=first.next_cursor)

        assert [post.title for post in first] == ["post-01", "post-02", "post-03", "post-04"]
        assert [post.title for post in second] == ["post-05", "post-06", "post-07", "post-08"]

    async def test_final_page_has_no_cursor(self, query: OpenQuery, many_posts: list[Post]) -> None:
        page = await query(Post).cursor_page(size=100, by=[Post.id])

        assert len(page) == 10
        assert page.next_cursor is None

    async def test_discards_an_existing_order(self, query: OpenQuery, many_posts: list[Post]) -> None:
        page = await query(Post).order_by(Post.title.desc()).cursor_page(size=2, by=[Post.id])

        assert [post.title for post in page] == ["post-01", "post-02"]

    async def test_respects_filters(self, query: OpenQuery, many_posts: list[Post]) -> None:
        page = await query(Post).where(Post.title == "post-05").cursor_page(size=10, by=[Post.id])

        assert [post.title for post in page] == ["post-05"]

    async def test_zero_size_returns_an_empty_final_page(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        page = await query(Post).cursor_page(size=0, by=[Post.id])

        assert list(page) == []
        assert page.next_cursor is None
        assert page.has_next is False
