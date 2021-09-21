import pytest
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.exc import DatabaseError

from aerie.models import metadata
from aerie.session import DbSession
from tests.conftest import databases

sample_table = sa.Table('sample', metadata, sa.Column(sa.Integer, name='id'))


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
def test_returns_session(db):
    assert isinstance(db.session(), DbSession)


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_creates_tables(db):
    await db.create_tables()
    async with db.session() as session:
        await session.execute(text('select * from sample'))
        assert True  # DatabaseError has not been raised -> table exists


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_drops_tables(db):
    await db.create_tables(['sample'])
    await db.drop_tables(['sample'])
    async with db.session() as session:
        with pytest.raises(DatabaseError):
            await session.execute(text('select * from sample'))


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_transaction(db):
    async with db.transaction() as tx:
        result = await tx.execute(text('select 1'))
    assert result.scalar_one() == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_executes_dsl(db):
    await db.execute(text('select 1'))


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_executes_string_query(db):
    await db.execute('select 1')
