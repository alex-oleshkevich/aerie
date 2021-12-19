import pytest
from sqlalchemy import select

from aerie.database import Aerie
from tests.conftest import databases
from tests.tables import Profile, profile_table, User, users_table


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_where(db: Aerie) -> None:
    results = await db.query(users_table).where(users_table.c.id == 1).all()
    assert len(results) == 1
    assert results[0]['id'] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_session_where(db: Aerie) -> None:
    async with db.session() as session:
        results = await session.query(User).where(User.id == 1).all()
        assert len(results) == 1
        assert results[0].id == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_having(db: Aerie) -> None:
    results = await db.query(users_table).having(users_table.c.id == 1).group_by(users_table.c.id).all()
    assert len(results) == 1
    assert results[0]['id'] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_session_having(db: Aerie) -> None:
    async with db.session() as session:
        results = await session.query(User).having(User.id == 1).group_by(User.id).all()
        assert len(results) == 1
        assert results[0].id == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_order_by(db: Aerie) -> None:
    results = await db.query(users_table).order_by(users_table.c.id.desc()).all()
    assert len(results) == 3
    assert results[0]['id'] == 3


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_session_order_by(db: Aerie) -> None:
    async with db.session() as session:
        results = await session.query(User).order_by(User.id.desc()).all()
        assert len(results) == 3
        assert results[0].id == 3


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_limit(db: Aerie) -> None:
    results = await db.query(users_table).limit(1).all()
    assert len(results) == 1
    assert results[0]['id'] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_session_limit(db: Aerie) -> None:
    async with db.session() as session:
        results = await session.query(User).limit(1).all()
        assert len(results) == 1
        assert results[0].id == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_limit_with_offset(db: Aerie) -> None:
    results = await db.query(users_table).limit(1, 1).all()
    assert len(results) == 1
    assert results[0]['id'] == 2


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_session_limit_with_offset(db: Aerie) -> None:
    async with db.session() as session:
        results = await session.query(User).limit(1, 1).all()
        assert len(results) == 1
        assert results[0].id == 2


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_offset(db: Aerie) -> None:
    results = await db.query(users_table).offset(1).all()
    assert len(results) == 2
    assert results[0]['id'] == 2


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_session_offset(db: Aerie) -> None:
    async with db.session() as session:
        results = await session.query(User).offset(1).all()
        assert len(results) == 2
        assert results[0].id == 2


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_slice(db: Aerie) -> None:
    results = await db.query(users_table).slice(0, 2).all()
    assert len(results) == 2
    assert results[0]['id'] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_session_slice(db: Aerie) -> None:
    async with db.session() as session:
        results = await session.query(User).slice(0, 2).all()
        assert len(results) == 2
        assert results[0].id == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_preload_one_to_one(db: Aerie) -> None:
    async with db.session() as session:
        result = await session.query(User).preload(User.profile).first()
        assert result
        assert result.profile.first_name == 'User'


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_prefetch_many_to_many(db: Aerie) -> None:
    async with db.session() as session:
        result = await session.query(User).prefetch(User.addresses).first()
        assert result
        assert len(result.addresses) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_join(db: Aerie) -> None:
    results = await db.query(users_table).join(profile_table).order_by(profile_table.c.last_name.desc()).all()
    assert results[0]['id'] == 2
    assert len(results) == 2  # user 3 has no profile associated


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_session_join(db: Aerie) -> None:
    async with db.session() as session:
        results = await session.query(User).join(User.profile).order_by(Profile.last_name.desc()).all()
        assert results[0].id == 2
        assert len(results) == 2  # user 3 has no profile associated


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_left_join(db: Aerie) -> None:
    results = await db.query(users_table).left_join(profile_table).order_by(profile_table.c.last_name.desc()).all()
    assert len(results) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_session_left_join(db: Aerie) -> None:
    async with db.session() as session:
        results = await session.query(User).left_join(Profile).order_by(Profile.last_name.desc()).all()
        assert len(results) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_only(db: Aerie) -> None:
    result = await db.query(users_table).only(users_table.c.id).first()
    assert result
    assert 'User One' not in result


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_only_with_session(db: Aerie) -> None:
    async with db.session() as session:
        result = await session.query(User).only(User.id).first()
        assert result
        assert 'User One' not in result


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_filter_by(db: Aerie) -> None:
    results = await db.query(users_table).filter_by(name='User Two').all()
    assert len(results) == 1
    assert results[0]['name'] == 'User Two'


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_session_filter_by(db: Aerie) -> None:
    async with db.session() as session:
        results = await session.query(User).filter_by(name='User Two').all()
        assert len(results) == 1
        assert results[0].name == 'User Two'
