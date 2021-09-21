import pytest

from aerie.database import Aerie
from aerie.exceptions import NoResultsError, TooManyResultsError
from aerie.session import DbSession
from tests.conftest import DATABASE_URLS, User


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_first(url):
    db = Aerie(url)
    async with db.session() as session:
        stmt = session.select(User)
        user = await session.first(stmt)
        assert user.id == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_one(url):
    db = Aerie(url)
    async with db.session() as session:
        stmt = session.select(User).where(User.id == 1)
        user = await session.one(stmt)
        assert user.id == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_one_when_many_results(url):
    db = Aerie(url)
    async with db.session() as session:
        with pytest.raises(TooManyResultsError):
            stmt = session.select(User)
            await session.one(stmt)


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_one_when_no_results(url):
    db = Aerie(url)
    async with db.session() as session:
        with pytest.raises(NoResultsError):
            stmt = session.select(User).where(User.id == -1)
            await session.one(stmt)


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_one_or_none(url):
    db = Aerie(url)
    async with db.session() as session:
        stmt = session.select(User).where(User.id == 1)
        user = await session.one_or_none(stmt)
        assert user.id == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_one_or_none_when_many_results(url):
    db = Aerie(url)
    async with db.session() as session:
        with pytest.raises(TooManyResultsError):
            stmt = session.select(User)
            await session.one_or_none(stmt)


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_one_or_none_when_no_results(url):
    db = Aerie(url)
    async with db.session() as session:
        stmt = session.select(User).where(User.id == -1)
        assert await session.one_or_none(stmt) is None


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_all(url):
    db = Aerie(url)
    async with db.session() as session:
        stmt = session.select(User)
        user = await session.all(stmt)
        assert len(user) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_count(url):
    db = Aerie(url)
    async with db.session() as session:
        stmt = session.select(User)
        assert await session.count(stmt) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_exists(url):
    db = Aerie(url)
    async with db.session() as session:  # type: DbSession
        stmt = session.select(User).where(User.id == 1)
        assert await session.exists(stmt) is True

        stmt = session.select(User).where(User.id == -1)
        assert await session.exists(stmt) is False


@pytest.mark.asyncio
@pytest.mark.parametrize('url', DATABASE_URLS)
async def test_add(url):
    db = Aerie(url)
    async with db.session() as session:  # type: DbSession
        session.add(User(id=4, name='User Four'))
        await session.flush()
        assert await session.exists(session.select(User).where(User.id == 4))
