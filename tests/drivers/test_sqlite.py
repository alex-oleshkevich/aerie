import pytest

from aerie.drivers.sqlite import SQLiteDriver, _Connection
from aerie.url import URL


@pytest.fixture(scope='session')
def driver():
    return SQLiteDriver(URL('sqlite:///tmp/example3.db'))


@pytest.fixture(autouse=True)
async def database(driver):
    async with driver.connect() as conn:
        await conn.execute(
            'create table if not exists users '
            '(id integer primary key, name text)'
        )
        yield
        await conn.execute('drop table users')


@pytest.fixture()
async def connection(driver):
    async with driver.connect() as connection:
        yield connection


@pytest.mark.asyncio
async def test_execute(connection: _Connection):
    result = await connection.execute(
        'insert into users (name) values ("user1")')
    assert result == 1


@pytest.mark.asyncio
async def test_execute_many_and_fetch_val(connection: _Connection):
    values = [
        dict(name='user1'),
        dict(name='user2'),
        dict(name='user3'),
    ]
    await connection.execute_many(
        'insert into users (name) values (":name")', values
    )
    count = await connection.fetch_val('select count(*) from users')
    assert count == 3


@pytest.mark.asyncio
async def test_fetch_one(connection: _Connection):
    await connection.execute('insert into users (name) values ("user1")')
    row = await connection.fetch_one('select * from users limit 1')
    assert dict(row) == dict(id=1, name='user1')


@pytest.mark.asyncio
async def test_fetch_all(connection: _Connection):
    values = [dict(name='user1'), dict(name='user2')]
    await connection.execute_many(
        'insert into users (name) values (:name)', values
    )
    rows = await connection.fetch_all('select * from users')
    assert list(map(dict, rows)) == [
        dict(id=1, name='user1'), dict(id=2, name='user2')
    ]


@pytest.mark.asyncio
async def test_iterate(connection: _Connection):
    values = [dict(name='user1'), dict(name='user2')]
    await connection.execute_many(
        'insert into users (name) values (:name)', values
    )
    iterator = connection.iterate('select * from users')
    row = await iterator.__anext__()
    assert dict(row) == dict(id=1, name='user1')

    row = await iterator.__anext__()
    assert dict(row) == dict(id=2, name='user2')

    with pytest.raises(StopAsyncIteration):
        await iterator.__anext__()


@pytest.mark.asyncio
async def test_transaction_commit(connection: _Connection):
    assert await connection.fetch_val('select count(*) from users') == 0

    async with connection.transaction():
        values = [dict(name='user1'), dict(name='user2')]
        await connection.execute_many(
            'insert into users (name) values (:name)', values
        )

    assert await connection.fetch_val('select count(*) from users') == 2


@pytest.mark.asyncio
async def test_transaction_commit_manual(connection: _Connection):
    assert await connection.fetch_val('select count(*) from users') == 0

    tx = connection.transaction()
    await tx.begin()
    await connection.execute('insert into users (name) values ("user")')
    await tx.commit()

    assert await connection.fetch_val('select count(*) from users') == 1


@pytest.mark.asyncio
async def test_transaction_rollback(connection: _Connection):
    assert await connection.fetch_val('select count(*) from users') == 0

    try:
        async with connection.transaction():
            values = [dict(name='user1'), dict(name='user2')]
            await connection.execute_many(
                'insert into users (name) values (:name)', values
            )
            raise Exception()
    except Exception:
        pass

    assert await connection.fetch_val('select count(*) from users') == 0


@pytest.mark.asyncio
async def test_transaction_rollback_manual(connection: _Connection):
    assert await connection.fetch_val('select count(*) from users') == 0

    tx = connection.transaction()
    await tx.begin()
    await connection.execute('insert into users (name) values ("user")')
    await tx.rollback()

    assert await connection.fetch_val('select count(*) from users') == 0


@pytest.mark.asyncio
async def test_nested_transactions(connection: _Connection):
    assert await connection.fetch_val('select count(*) from users') == 0

    async def _insert():
        await connection.execute('insert into users (name) values ("user")')

    async def _count():
        return await connection.fetch_val('select count(*) from users')

    async with connection.transaction() as tx:
        async with tx.savepoint('sp1'):
            await _insert()  # count = 1

        assert await _count() == 1

        async with tx.savepoint('sp2') as sp2:
            await _insert()  # count = 2
            assert await _count() == 2
            await sp2.rollback()
            assert await _count() == 1
