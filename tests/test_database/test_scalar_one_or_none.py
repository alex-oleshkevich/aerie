import pytest
from sqlalchemy import select

from aerie import Aerie, TooManyResultsError
from tests.conftest import DATABASE_URLS, users


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_scalar_one_or_none_dsl(url):
    db = Aerie(url)
    stmt = select(users).where(users.c.id == 1)
    row = await db.fetch_scalar_or_none(stmt)
    assert row == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_scalar_one_or_none_string_query(url):
    db = Aerie(url)
    row = await db.fetch_scalar_or_none('select * from users where id = 1')
    assert row == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_scalar_or_none_one_many_results(url):
    db = Aerie(url)
    with pytest.raises(TooManyResultsError):
        await db.fetch_scalar_or_none('select * from users')


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_scalar_or_none_one_no_results(url):
    db = Aerie(url)
    assert await db.fetch_scalar_or_none('select * from users where id = -1') is None
