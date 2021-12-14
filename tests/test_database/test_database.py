import pytest
import sqlalchemy as sa
from sqlalchemy import select, text
from sqlalchemy.exc import DatabaseError

from aerie import Aerie
from aerie.models import metadata
from aerie.session import DbSession
from tests.conftest import databases, users

sample_table = sa.Table('sample', metadata, sa.Column(sa.Integer, name='id'))


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
def test_returns_session(db: Aerie) -> None:
    assert isinstance(db.session(), DbSession)


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_creates_tables(db: Aerie) -> None:
    await db.schema.create_tables()
    async with db.session() as session:
        await session.execute(text('select * from sample'))
        assert True  # DatabaseError has not been raised -> table exists


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_drops_tables(db: Aerie) -> None:
    await db.schema.create_tables(['sample'])
    await db.schema.drop_tables(['sample'])
    async with db.session() as session:
        with pytest.raises(DatabaseError):
            await session.execute(text('select * from sample'))


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_transaction(db: Aerie) -> None:
    async with db.transaction() as tx:
        result = await tx.execute(text('select 1'))
    assert result.scalar_one() == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_executes(db: Aerie) -> None:
    await db.query(text('select 1')).execute()


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_count(db: Aerie) -> None:
    stmt = select(users).where(users.c.id == 1)
    assert await db.query(stmt).count() == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_exists(db: Aerie) -> None:
    stmt = select(users).where(users.c.id == 1)
    assert await db.query(stmt).exists() is True

    stmt = select(users).where(users.c.id == -1)
    assert await db.query(stmt).exists() is False


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_first(db: Aerie) -> None:
    stmt = select(users).where(users.c.id == 1)
    user = await db.query(stmt).first()
    assert user
    assert user.id == 1


def test_shares_instances() -> None:
    db = Aerie('sqlite+aiosqlite:///:memory:', name='instance1')
    db2 = Aerie('sqlite+aiosqlite:///:memory:', name='instance2')

    assert Aerie.get_instance('instance1') == db
    assert Aerie.get_instance('instance2') == db2
