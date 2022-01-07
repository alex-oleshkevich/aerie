import pytest
import sqlalchemy as sa

from aerie.database import Aerie
from tests.tables import AutoBigIntModel, AutoIntModel, User, UserToAddress


@pytest.mark.asyncio
async def test_model_all(db: Aerie) -> None:
    async with db.session():
        users = await User.all()
        assert len(users) == 3


@pytest.mark.asyncio
async def test_model_creates_query(db: Aerie) -> None:
    async with db.session():
        user = await User.first()
        assert user
        assert user.id == 1


@pytest.mark.asyncio
async def test_model_get(db: Aerie) -> None:
    async with db.session():
        user = await User.get(1)
        assert user.id == 1


@pytest.mark.asyncio
async def test_model_first(db: Aerie) -> None:
    async with db.session():
        user = await User.first()
        assert user
        assert user.id == 1


@pytest.mark.asyncio
async def test_model_refresh(db: Aerie) -> None:
    async with db.session() as session:
        user = await User.create()
        await User.query().where(User.id == user.id).update(name='updated')
        await user.refresh()
        assert user.name == 'updated'
        await session.rollback()


@pytest.mark.asyncio
async def test_model_get_or_none(db: Aerie) -> None:
    async with db.session():
        user = await User.get_or_none(100500)
        assert not user


@pytest.mark.asyncio
async def test_model_get_or_create(db: Aerie) -> None:
    async with db.session() as session:
        user = await User.get_or_create(User.name == 'autocreated', {'name': 'autocreated'})
        assert user.name == 'autocreated'
        await session.rollback()


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


@pytest.mark.asyncio
async def test_model_destroy(db: Aerie) -> None:
    async with db.session():
        user = await User.create(id=100, name='Hundred')
        await User.destroy(user.id)
        assert await User.query().where(User.id == user.id).exists() is False


def test_autointeger_id() -> None:
    info = sa.inspect(AutoBigIntModel)
    assert hasattr(AutoIntModel, 'id')
    assert isinstance(info.c.id.type, sa.Integer)


def test_bigautointeger_id() -> None:
    info = sa.inspect(AutoBigIntModel)
    assert hasattr(AutoBigIntModel, 'id')
    assert isinstance(info.c.id.type, sa.BigInteger)


@pytest.mark.asyncio
async def test_repr(db: Aerie) -> None:
    async with db.session():
        new_user = User(id=42, name='user')
        assert repr(new_user) == f'<User: transient {id(new_user)}>'

        user = await User.first()
        assert user
        assert repr(user) == '<User: pk=1>'

        many_pk = await UserToAddress.first()
        assert many_pk
        assert repr(many_pk) == '<UserToAddress: pk=(1, 1)>'
