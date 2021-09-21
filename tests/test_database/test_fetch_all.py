import pytest
from sqlalchemy import select

from aerie import Aerie
from tests.conftest import DATABASE_URLS, users


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_fetch_all_dsl(url):
    db = Aerie(url)
    rows = await db.fetch_all(select(users))
    assert len(rows) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_fetch_all_string_query(url):
    db = Aerie(url)
    rows = await db.fetch_all('select * from users')
    assert rows[0].id == 1
    assert len(rows) == 3
