import pytest
from sqlalchemy import select

from aerie import Aerie, NoResultsError, TooManyResultsError
from tests.conftest import databases, users


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_scalar_one(db: Aerie) -> None:
    stmt = select(users).where(users.c.id == 1)
    row = await db.query(stmt).scalar()
    assert row == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_scalar_one_many_results(db: Aerie) -> None:
    with pytest.raises(TooManyResultsError):
        stmt = select(users)
        await db.query(stmt).scalar()


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_scalar_one_no_results(db: Aerie) -> None:
    with pytest.raises(NoResultsError):
        stmt = select(users).where(users.c.id == -1)
        await db.query(stmt).scalar()
