import pytest
from sqlalchemy import select

from aerie import Aerie, NoResultsError, TooManyResultsError
from tests.conftest import DATABASE_URLS, users


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_scalar_one_dsl(url):
    db = Aerie(url)
    stmt = select(users).where(users.c.id == 1)
    row = await db.fetch_scalar(stmt)
    assert row == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_scalar_one_string_query(url):
    db = Aerie(url)
    row = await db.fetch_scalar('select * from users where id = 1')
    assert row == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_scalar_one_many_results(url):
    db = Aerie(url)
    with pytest.raises(TooManyResultsError):
        await db.fetch_scalar('select * from users')


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_scalar_one_no_results(url):
    db = Aerie(url)
    with pytest.raises(NoResultsError):
        await db.fetch_scalar('select * from users where id = -1')
