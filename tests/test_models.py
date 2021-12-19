import pytest

from aerie.database import Aerie
from tests.tables import User


@pytest.mark.asyncio
async def test_model_creates_query(db: Aerie) -> None:
    async with db.session():
        user = await User.query().first()
        assert user
        assert user.id == 1


@pytest.mark.asyncio
async def test_model_get(db: Aerie) -> None:
    async with db.session():
        user = await User.get(1)
        assert user.id == 1


@pytest.mark.asyncio
async def test_model_get_or_none(db: Aerie) -> None:
    async with db.session():
        user = await User.get_or_none(100500)
        assert not user


@pytest.mark.asyncio
async def test_model_create_delete(db: Aerie) -> None:
    async with db.session():
        user = await User.create(id=100, name='Hundred')

        db_user = await User.get(pk=user.id)
        assert db_user.name == user.name

        await user.delete()
        assert not await User.query().where(User.id == user.id).exists()


@pytest.mark.asyncio
async def test_model_create_via_save(db: Aerie) -> None:
    async with db.session():
        user = User(id=100, name='Hundred')
        await user.save()

        db_user = await User.get(pk=user.id)
        assert db_user.name == user.name

        await user.delete()


@pytest.mark.asyncio
async def test_model_save(db: Aerie) -> None:
    async with db.session():
        user = User(id=100, name='Hundred')
        await user.save()

        db_user = await User.get(pk=user.id)
        db_user.name = 'changed'
        await db_user.save()

        db_user = await User.get(pk=user.id)
        assert db_user.name == 'changed'

        await user.delete()
