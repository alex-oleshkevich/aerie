import pytest

from aerie import NoResultsError, TooManyResultsError
from aerie.database import Aerie
from tests.conftest import databases
from tests.tables import Address, User


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_all(db: Aerie) -> None:
    async with db.session() as session:
        users = await session.query(User).all()
        assert len(users) == 3
        assert isinstance(users[0], User)


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_choices(db: Aerie) -> None:
    async with db.session() as session:
        users = await session.query(User).choices()
        assert len(users) == 3
        assert users[0] == (1, 'User One')


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_choices_dict(db: Aerie) -> None:
    async with db.session() as session:
        users = await session.query(User).choices_dict()
        assert len(users) == 3
        assert users[0] == {'value': 1, 'label': 'User One'}


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_query_evaluates_to_all(db: Aerie) -> None:
    async with db.session() as session:
        users = await session.query(User)
        assert len(users) == 3
        assert isinstance(users[0], User)


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_first(db: Aerie) -> None:
    async with db.session() as session:
        user = await session.query(User).first()
        assert user
        assert isinstance(user, User)
        assert user.id == 1
        assert user.name == 'User One'


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_one(db: Aerie) -> None:
    async with db.session() as session:
        user = await session.query(User).where(User.id == 1).one()
        assert user.id == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_one_when_many_results(db: Aerie) -> None:
    async with db.session() as session:
        with pytest.raises(TooManyResultsError):
            await session.query(User).one()


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_one_when_no_results(db: Aerie) -> None:
    async with db.session() as session:
        with pytest.raises(NoResultsError):
            await session.query(User).where(User.id == -1).one()


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_one_or_none(db: Aerie) -> None:
    async with db.session() as session:
        user = await session.query(User).where(User.id == 1).one_or_none()
        assert user
        assert user.id == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_one_or_none_when_many_results(db: Aerie) -> None:
    async with db.session() as session:
        with pytest.raises(TooManyResultsError):
            await session.query(User).one_or_none()


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_one_or_none_when_no_results(db: Aerie) -> None:
    async with db.session() as session:
        assert await session.query(User).where(User.id == -1).one_or_none() is None


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_count(db: Aerie) -> None:
    async with db.session() as session:
        assert await session.query(User).count() == 3


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_exists(db: Aerie) -> None:
    async with db.session() as session:
        assert await session.query(User).where(User.id == 1).exists() is True
        assert await session.query(User).where(User.id == -1).exists() is False


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_update(db: Aerie) -> None:
    async with db.session() as session:
        address = Address(id=10, city='10', street='10')
        session.add(address)
        await session.flush([address])
        await session.query(Address).where(Address.city == '10').update(city='30')

        assert await session.query(Address).where(Address.city == '10').exists() is False
        assert await session.query(Address).where(Address.city == '30').exists() is True


@pytest.mark.asyncio
@pytest.mark.parametrize('db', databases)
async def test_delete(db: Aerie) -> None:
    async with db.session() as session:
        address = Address(id=10, city='10', street='10')
        session.add(address)
        await session.flush([address])
        assert await session.query(Address).where(Address.city == '10').exists() is True

        await session.query(Address).where(Address.city == '10').delete()
        assert await session.query(Address).where(Address.city == '10').exists() is False
