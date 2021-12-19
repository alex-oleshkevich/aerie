import pytest

from aerie import Aerie, DbSession
from aerie.exceptions import NoActiveSessionError
from tests.tables import User


@pytest.mark.asyncio
async def test_adds_multiple_objects(db: Aerie) -> None:
    async with db.session() as session:
        user = User()
        user2 = User()
        session.add(user, user2)
    await session.commit()


@pytest.mark.asyncio
async def test_session_maintains_stack(db: Aerie) -> None:
    async with db.session() as session:
        assert len(DbSession.current_session_stack.get()) == 1
        assert DbSession.get_current_session() == session
        async with db.session() as session2:
            assert len(DbSession.current_session_stack.get()) == 2
            assert DbSession.get_current_session() == session2
            await session.query(User).where(User.id == 1).all()
        assert len(DbSession.current_session_stack.get()) == 1
    assert len(DbSession.current_session_stack.get()) == 0

    with pytest.raises(NoActiveSessionError):
        assert DbSession.get_current_session() is None
