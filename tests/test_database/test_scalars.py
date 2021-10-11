import pytest
from sqlalchemy import select

from aerie.database import Aerie
from tests.conftest import databases, users


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_scalars(db: Aerie) -> None:
    stmt = select(users.c.id).where(users.c.id == 1)
    rows = await db.query(stmt).scalars()
    assert rows[0] == 1
