import pytest

from aerie import TooManyResultsError
from tests.conftest import databases


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_fetch_one_or_none(db):
    row = await db.fetch_one_or_none('select * from users where id = 1')
    assert row.id == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_fetch_one_or_none_many_results(db):
    with pytest.raises(TooManyResultsError):
        await db.fetch_one_or_none('select * from users')


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_fetch_one_or_none_no_results(db):
    assert await db.fetch_one_or_none('select * from users where id = -1') is None
