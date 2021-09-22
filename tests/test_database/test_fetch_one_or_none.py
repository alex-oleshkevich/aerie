import pytest

from aerie import TooManyResultsError
from tests.conftest import databases


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_fetch_one_or_none(db):
    row = await db.query('select * from users where id = 1').one_or_none()
    assert row.id == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_fetch_one_or_none_many_results(db):
    with pytest.raises(TooManyResultsError):
        await db.query('select * from users').one_or_none()


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_fetch_one_or_none_no_results(db):
    assert await db.query('select * from users where id = -1').one_or_none() is None
