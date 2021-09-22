import pytest
from sqlalchemy import select

from tests.conftest import databases, users


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_fetch_all(db):
    rows = await db.query(select(users)).all()
    assert len(rows) == 3
