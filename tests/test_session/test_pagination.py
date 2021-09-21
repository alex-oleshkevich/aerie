import pytest

from aerie.database import Aerie
from aerie.session import Page
from tests.conftest import DATABASE_URLS, User


def test_page() -> None:
    rows = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    page = Page(rows, total_rows=101, page=2, page_size=10)
    assert page.total_pages == 11
    assert page.has_next
    assert page.has_previous
    assert page.previous_page == 1
    assert page.next_page == 3
    assert page.start_index == 11
    assert page.end_index == 20

    assert bool(page)
    assert page[0] == 1

    with pytest.raises(IndexError):
        assert rows[11]


def test_page_iterator() -> None:
    rows = [1, 2]
    page = Page(rows, total_rows=2, page=1, page_size=2)
    assert rows == [p for p in page]
    assert next(page) == 1
    assert next(page) == 2

    with pytest.raises(StopIteration):
        assert next(page) == 3


def test_page_start_index_for_first_page() -> None:
    rows = [1, 2]
    page = Page(rows, total_rows=2, page=1, page_size=2)
    assert page.start_index == 1
    assert page.end_index == 2


def test_page_next_prev_pages() -> None:
    rows = [1, 2]
    page = Page(rows, total_rows=1, page=1, page_size=1)
    assert page.has_next is False
    assert page.has_previous is False
    assert page.next_page == 1
    assert page.previous_page == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_paginate(url: str) -> None:
    db = Aerie(url)
    async with db.session() as session:
        stmt = session.select(User)
        page = await session.paginate(stmt, 2, 1)
        assert page.total_rows == 3
        assert len(page) == 1
        assert next(page).id == 2
