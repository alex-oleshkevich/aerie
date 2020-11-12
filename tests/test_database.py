import aerie
import pytest

aerie.Database.register_driver('dummy', 'tests.test_database.DummyDriver')


class DummyDriver():
    pass


@pytest.fixture()
def db():
    return aerie.Database('dummy://')


@pytest.mark.asyncio
async def test_execute(db):
    pass
