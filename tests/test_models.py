import pytest

from aerie.database import Aerie
from tests.tables import User


@pytest.mark.asyncio
async def test_model_creates_query(db: Aerie) -> None:
    async with db.session():
        user = await User.query().first()
        assert user
        assert user.id == 1
