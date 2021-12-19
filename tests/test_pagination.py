import pytest

from aerie import Aerie, Page
from tests.conftest import databases
from tests.tables import User, users_table


def test_page() -> None:
    rows = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    page = Page(rows, total_rows=101, page=2, page_size=10)
    assert page.total_pages == 11
    assert page.has_next
    assert page.has_previous
    assert page.has_other
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


def test_page_no_other_pages() -> None:
    page: Page = Page([], total_rows=2, page=1, page_size=2)
    assert not page.has_other


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
@pytest.mark.parametrize('db', databases)
async def test_paginate(db: Aerie) -> None:
    page = await db.query(users_table).paginate(2, 1)
    assert page.total_rows == 3
    assert len(page) == 1
    assert next(page).id == 2


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_paginate_session(db: Aerie) -> None:
    async with db.session() as session:
        page = await session.query(User).paginate(2, 1)
        assert page.total_rows == 3
        assert len(page) == 1
        assert next(page).id == 2


def test_page_repr() -> None:
    rows = [1, 2]
    page = Page(rows, total_rows=2, page=1, page_size=2)
    assert repr(page) == '<Page: page=1, total_pages=1>'


def test_page_str() -> None:
    rows = [1, 2]
    page = Page(rows, total_rows=2, page=1, page_size=2)
    assert str(page) == 'Page 1 of 1, rows 1 - 2 of 2.'
