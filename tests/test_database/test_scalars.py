import pytest
from sqlalchemy import select

from aerie import Aerie
from tests.conftest import DATABASE_URLS, users


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_scalars_dsl(url):
    db = Aerie(url)
    stmt = select(users.c.id).where(users.c.id == 1)
    rows = await db.fetch_scalars(stmt)
    assert rows[0] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_scalars_string_query(url):
    db = Aerie(url)
    rows = await db.fetch_scalars('select * from users where id = 1')
    assert rows[0] == 1
