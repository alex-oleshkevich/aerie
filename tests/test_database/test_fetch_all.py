import pytest
from sqlalchemy import select

from tests.conftest import databases, users


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_fetch_all_dsl(db):
    rows = await db.query(select(users)).all()
    assert len(rows) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_fetch_all_string_query(db):
    rows = await db.query('select * from users').all()
    assert rows[0].id == 1
    assert len(rows) == 3
