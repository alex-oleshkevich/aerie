import pytest
from sqlalchemy import select

from aerie import NoResultsError, TooManyResultsError
from tests.conftest import databases, users


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_scalar_one_dsl(db):
    stmt = select(users).where(users.c.id == 1)
    row = await db.query(stmt).scalar()
    assert row == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_scalar_one_string_query(db):
    row = await db.query('select * from users where id = 1').scalar()
    assert row == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_scalar_one_many_results(db):
    with pytest.raises(TooManyResultsError):
        await db.query('select * from users').scalar()


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_scalar_one_no_results(db):
    with pytest.raises(NoResultsError):
        await db.query('select * from users where id = -1').scalar()
