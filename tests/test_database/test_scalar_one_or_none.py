import pytest
from sqlalchemy import select

from aerie import Aerie, TooManyResultsError
from tests.conftest import databases, users


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_scalar_one_or_none(db: Aerie) -> None:
    stmt = select(users).where(users.c.id == 1)
    row = await db.query(stmt).scalar_or_none()
    assert row == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_scalar_or_none_one_many_results(db: Aerie) -> None:
    with pytest.raises(TooManyResultsError):
        stmt = select(users)
        await db.query(stmt).scalar_or_none()


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_scalar_or_none_one_no_results(db: Aerie) -> None:
    stmt = select(users).where(users.c.id == -1)
    assert await db.query(stmt).scalar_or_none() is None
