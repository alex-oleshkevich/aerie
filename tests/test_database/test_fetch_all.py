import pytest
from sqlalchemy import select

from aerie import Aerie
from tests.conftest import databases, users


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_fetch_all(db: Aerie) -> None:
    rows = await db.query(select(users)).all()
    assert len(rows) == 3
