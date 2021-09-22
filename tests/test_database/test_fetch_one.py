import pytest
from sqlalchemy import select

from aerie import NoResultsError, TooManyResultsError
from tests.conftest import databases, users


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_fetch_one(db):
    stmt = select(users).where(users.c.id == 1)
    row = await db.query(stmt).one()
    assert row.id == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_fetch_one_many_results(db):
    with pytest.raises(TooManyResultsError):
        stmt = select(users)
        await db.query(stmt).one()


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_fetch_one_no_results(db):
    with pytest.raises(NoResultsError):
        stmt = select(users).where(users.c.id == -1)
        await db.query(stmt).one()
