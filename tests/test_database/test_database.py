import pytest
import sqlalchemy as sa
from sqlalchemy import select, text
from sqlalchemy.exc import DatabaseError

from aerie.models import metadata
from aerie.session import DbSession
from tests.conftest import databases, users

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
    await db.query(text('select 1')).execute()


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_executes_string_query(db):
    await db.query('select 1').execute()


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_count_dsl(db):
    stmt = select(users).where(users.c.id == 1)
    assert await db.query(stmt).count() == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_count_string_query(db):
    assert await db.query('select * from users').count() == 3


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_exists_dsl(db):
    stmt = select(users).where(users.c.id == 1)
    assert await db.query(stmt).exists() is True

    stmt = select(users).where(users.c.id == -1)
    assert await db.query(stmt).exists() is False


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_exists_string_query(db):
    assert await db.query('select * from users where id = 1').exists() is True
    assert await db.query('select * from users where id = -1').exists() is False
