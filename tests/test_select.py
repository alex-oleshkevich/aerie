import pytest

from aerie import NoResultsError, TooManyResultsError
from aerie.database import Aerie
from tests.conftest import databases
from tests.tables import User


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_all(db: Aerie) -> None:
    async with db.session() as session:
        async with session:
            users = await session.query(User).all()
            assert len(users) == 3
            assert isinstance(users[0], User)


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_query_evaluates_to_all(db: Aerie) -> None:
    async with db.session() as session:
        async with session:
            users = await session.query(User)
            assert len(users) == 3
            assert isinstance(users[0], User)


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_first(db: Aerie) -> None:
    async with db.session() as session:
        async with session:
            user = await session.query(User).first()
            assert user
            assert isinstance(user, User)
            assert user.id == 1
            assert user.name == 'User One'


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_one(db: Aerie) -> None:
    async with db.session() as session:
        async with session:
            user = await session.query(User).where(User.id == 1).one()
            assert user.id == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_one_when_many_results(db: Aerie) -> None:
    async with db.session() as session:
        async with session:
            with pytest.raises(TooManyResultsError):
                await session.query(User).one()


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_one_when_no_results(db: Aerie) -> None:
    async with db.session() as session:
        async with session:
            with pytest.raises(NoResultsError):
                await session.query(User).where(User.id == -1).one()


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_one_or_none(db: Aerie) -> None:
    async with db.session() as session:
        async with session:
            user = await session.query(User).where(User.id == 1).one_or_none()
            assert user
            assert user.id == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_one_or_none_when_many_results(db: Aerie) -> None:
    async with db.session() as session:
        async with session:
            with pytest.raises(TooManyResultsError):
                await session.query(User).one_or_none()


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_one_or_none_when_no_results(db: Aerie) -> None:
    async with db.session() as session:
        async with session:
            assert await session.query(User).where(User.id == -1).one_or_none() is None


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_count(db: Aerie) -> None:
    async with db.session() as session:
        async with session:
            assert await session.query(User).count() == 3


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_exists(db: Aerie) -> None:
    async with db.session() as session:
        async with session:
            assert await session.query(User).where(User.id == 1).exists() is True
            assert await session.query(User).where(User.id == -1).exists() is False
