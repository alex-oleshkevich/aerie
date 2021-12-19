import pytest

from aerie import Aerie
from tests.tables import User


@pytest.mark.asyncio
async def test_adds_multiple_objects(db: Aerie) -> None:
    async with db.session() as session:
        user = User()
        user2 = User()
        session.add(user, user2)
    await session.commit()
