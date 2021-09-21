import pytest

from aerie.database import Aerie
from aerie.session import Page
from tests.conftest import DATABASE_URLS, User


def test_page() -> None:
    rows = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    page = Page(rows, total_rows_count=101, page=2, page_size=10)
    assert page.total_pages == 11
    assert page.has_next
    assert page.has_previous
    assert page.previous_page == 1

    assert next(page) == 1
    assert rows == [p for p in page]

    assert rows[0] == 1

    with pytest.raises(IndexError):
        assert rows[11]


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_paginate(url: str) -> None:
    db = Aerie(url)
    async with db.session() as session:
        stmt = session.select(User)
        page = await session.paginate(stmt, 2, 1)
        assert page.total_rows_count == 3
        assert len(page) == 1
        assert next(page).id == 2
