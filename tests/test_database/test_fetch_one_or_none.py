import pytest

from aerie import Aerie, TooManyResultsError
from tests.conftest import DATABASE_URLS


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_fetch_one_or_none(url):
    db = Aerie(url)
    row = await db.fetch_one_or_none('select * from users where id = 1')
    assert row.id == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_fetch_one_or_none_many_results(url):
    db = Aerie(url)
    with pytest.raises(TooManyResultsError):
        await db.fetch_one_or_none('select * from users')


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_fetch_one_or_none_no_results(url):
    db = Aerie(url)
    assert await db.fetch_one_or_none('select * from users where id = -1') is None
