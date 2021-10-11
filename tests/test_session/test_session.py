import pytest
import typing as t

from aerie import Aerie
from aerie.exceptions import NoResultsError, TooManyResultsError
from aerie.session import DbSession
from tests.conftest import User, databases


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_all(db: Aerie) -> None:
    async with db.session() as session:
        stmt = session.select(User)
        user = await session.query(stmt).all()
        assert len(user) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_first(db: Aerie) -> None:
    async with db.session() as session:
        stmt = session.select(User)
        user = await session.query(stmt).first()
        assert isinstance(user, User)
        assert user.id == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_one(db: Aerie) -> None:
    async with db.session() as session:
        stmt = session.select(User).where(User.id == 1)
        user: User = await session.query(stmt).one()
        assert user.id == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_one_when_many_results(db: Aerie) -> None:
    async with db.session() as session:
        with pytest.raises(TooManyResultsError):
            stmt = session.select(User)
            await session.query(stmt).one()


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_one_when_no_results(db: Aerie) -> None:
    async with db.session() as session:
        with pytest.raises(NoResultsError):
            stmt = session.select(User).where(User.id == -1)
            await session.query(stmt).one()


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_one_or_none(db: Aerie) -> None:
    async with db.session() as session:
        stmt = session.select(User).where(User.id == 1)
        user: t.Optional[User] = await session.query(stmt).one_or_none()
        assert user
        assert user.id == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_one_or_none_when_many_results(db: Aerie) -> None:
    async with db.session() as session:
        with pytest.raises(TooManyResultsError):
            stmt = session.select(User)
            await session.query(stmt).one_or_none()


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_one_or_none_when_no_results(db: Aerie) -> None:
    async with db.session() as session:
        stmt = session.select(User).where(User.id == -1)
        assert await session.query(stmt).one_or_none() is None


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_count(db: Aerie) -> None:
    async with db.session() as session:
        stmt = session.select(User)
        assert await session.query(stmt).count() == 3


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_exists(db: Aerie) -> None:
    async with db.session() as session:  # type: DbSession
        stmt = session.select(User).where(User.id == 1)
        assert await session.query(stmt).exists() is True

        stmt = session.select(User).where(User.id == -1)
        assert await session.query(stmt).exists() is False


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_add(db: Aerie) -> None:
    async with db.session() as session:  # type: DbSession
        session.add(User(id=4, name='User Four'))
        await session.flush()
        assert await session.query(session.select(User).where(User.id == 4)).exists()
