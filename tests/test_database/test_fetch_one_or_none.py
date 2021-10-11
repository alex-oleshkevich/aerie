import pytest
from sqlalchemy import select

from aerie import Aerie, TooManyResultsError
from tests.conftest import databases, users


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_fetch_one_or_none(db: Aerie) -> None:
    stmt = select(users).where(users.c.id == 1)
    row = await db.query(stmt).one_or_none()
    assert row
    assert row.id == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_fetch_one_or_none_many_results(db: Aerie) -> None:
    with pytest.raises(TooManyResultsError):
        await db.query(stmt=select(users)).one_or_none()


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_fetch_one_or_none_no_results(db: Aerie) -> None:
    stmt = select(users).where(users.c.id == -1)
    assert await db.query(stmt).one_or_none() is None
