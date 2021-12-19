import pytest
from sqlalchemy import select

from aerie import Aerie, DbSession
from aerie.exceptions import NoActiveSessionError
from tests.tables import User


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


@pytest.mark.asyncio
async def test_session_executes_selects(db: Aerie) -> None:
    async with db.session() as session:
        stmt = select(User).where(User.id == 1)
        result = await session.execute(stmt)
        assert result.scalars().one().name == 'User One'
