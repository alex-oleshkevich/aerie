import pytest
from sqlalchemy import select

from aerie import TooManyResultsError
from tests.conftest import databases, users


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_scalar_one_or_none_dsl(db):
    stmt = select(users).where(users.c.id == 1)
    row = await db.fetch_scalar_or_none(stmt)
    assert row == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_scalar_one_or_none_string_query(db):
    row = await db.fetch_scalar_or_none('select * from users where id = 1')
    assert row == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_scalar_or_none_one_many_results(db):
    with pytest.raises(TooManyResultsError):
        await db.fetch_scalar_or_none('select * from users')


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_scalar_or_none_one_no_results(db):
    assert await db.fetch_scalar_or_none('select * from users where id = -1') is None
