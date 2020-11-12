import asyncio

import pytest

from aerie.pool import Pool


class _Connection:
    pass


class _Driver:
    async def create_connection(self):
        return _Connection()


@pytest.fixture()
def pool():
    driver = _Driver()
    return Pool(driver=driver, max_size=2)


@pytest.mark.asyncio
async def test_pool_creates_connection(pool):
    async with pool.acquire() as connection:
        assert isinstance(connection, _Connection)
    assert len(pool) == 1


@pytest.mark.asyncio
async def test_pool_reuses_free_connection(pool):
    connections = []
    async with pool.acquire() as connection_1:
        connections.append(connection_1)
        async with pool.acquire() as connection_2:
            connections.append(connection_2)
        async with pool.acquire() as connection_3:
            assert connection_3 in connections
    assert len(pool) == 2


@pytest.mark.asyncio
async def test_pool_waits_for_free_connection(pool):
    async def _query():
        async with pool.acquire():
            await asyncio.sleep(0.01)

    tasks = []
    tasks.append(asyncio.create_task(_query()))
    tasks.append(asyncio.create_task(_query()))
    tasks.append(asyncio.create_task(_query()))
    await asyncio.wait(tasks)
    assert len(pool) == 2


@pytest.mark.asyncio
async def test_pool_survives_exception(pool):
    connections = []
    async with pool.acquire() as connection_1:
        connections.append(connection_1)
        try:
            async with pool.acquire():
                raise Exception
        except:
            pass
    assert len(pool) == 2
