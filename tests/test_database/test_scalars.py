import pytest
from sqlalchemy import select

from tests.conftest import databases, users


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_scalars_dsl(db):
    stmt = select(users.c.id).where(users.c.id == 1)
    rows = await db.query(stmt).scalars()
    assert rows[0] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_scalars_string_query(db):
    rows = await db.query('select * from users where id = 1').scalars()
    assert rows[0] == 1
