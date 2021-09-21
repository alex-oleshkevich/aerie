import pytest
from sqlalchemy import select

from aerie import Aerie, NoResultsError, TooManyResultsError
from tests.conftest import DATABASE_URLS, users


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_fetch_one_dsl(url):
    db = Aerie(url)
    stmt = select(users).where(users.c.id == 1)
    row = await db.fetch_one(stmt)
    assert row.id == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_fetch_one_string_query(url):
    db = Aerie(url)
    row = await db.fetch_one('select * from users where id = 1')
    assert row.id == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_fetch_one_many_results(url):
    db = Aerie(url)
    with pytest.raises(TooManyResultsError):
        await db.fetch_one('select * from users')


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_fetch_one_no_results(url):
    db = Aerie(url)
    with pytest.raises(NoResultsError):
        await db.fetch_one('select * from users where id = -1')
