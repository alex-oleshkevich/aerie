import asyncio

import pytest

from aerie import Database
from aerie.url import URL

DATABASES = [
    URL('sqlite:///tmp/aerie.test.db'),
    URL('postgresql://postgres:postgres@localhost:5432/aerie_test'),
]


@pytest.yield_fixture(scope='module')
def event_loop(request):
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


async def create_database(database_url: URL):
    if database_url.driver == 'sqlite':
        import aiosqlite
        connection = await aiosqlite.connect(database_url.db_name)
        sql = (
            "create table if not exists users "
            "(id integer primary key, name varchar(256))"
        )
        await connection.execute(sql)
        await connection.close()
    elif database_url.driver == 'postgresql':
        import asyncpg
        connection = await asyncpg.connect(database_url.url)
        sql = (
            "create table if not exists public.users "
            "(id serial primary key, name varchar(256))"
        )
        await connection.execute(sql)
        await connection.close()


async def drop_database(database_url: URL):
    if database_url.driver == 'sqlite':
        import aiosqlite
        connection = await aiosqlite.connect(database_url.db_name, timeout=2)
        await connection.execute('drop table if exists users')
        await connection.close()
    elif database_url.driver == 'postgresql':
        import asyncpg
        connection = await asyncpg.connect(database_url.url)
        await connection.execute('drop table if exists users')
        await connection.close()


@pytest.fixture(autouse=True, scope='module')
@pytest.mark.asyncio
async def setup_databases():
    for database_url in DATABASES:
        await create_database(database_url)
    yield
    for database_url in DATABASES:
        await drop_database(database_url)


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_queries(database_url):
    async with Database(database_url) as db:
        async with db.transaction(force_rollback=True):
            # test execute and fetch_val
            await db.execute(
                'insert into users (name) values (:name)',
                {'name': 'root'}
            )
            assert await db.fetch_val(
                "select count(*) from users where name = :name",
                {'name': 'root'}
            ) == 1

            # test execute_all, fetch_all and fetch_one
            await db.execute_all(
                'insert into users (name) values (:name)',
                [{'name': 'user1'}, {'name': 'user2'}]
            )
            rows = await db.fetch_all(
                'select * from users where name in (:user_1, :user_2)',
                {'user_1': 'user1', 'user_2': 'user2'}
            )
            assert rows[0]['name'] == 'user1'
            assert rows[1]['name'] == 'user2'

            row = await db.fetch_one(
                'select * from users where name in (:user_1, :user_2) limit 1',
                {'user_1': 'user1', 'user_2': 'user2'}
            )

            assert row['name'] == 'user1'

            # test iterate
            iterate_result = []
            async for row in db.iterate('select * from users'):
                iterate_result.append(row)

            assert len(iterate_result) == 3
            assert iterate_result[0]['name'] == 'root'
            assert iterate_result[1]['name'] == 'user1'
            assert iterate_result[2]['name'] == 'user2'

            print('HERE')


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_transaction_commit(database_url):
    """
    Given an empty database
    When I start transaction
    And insert one object
    And no exception raised
    Then the transaction has to be commited
    And total count of rows must be equal to 1.
    """
    async with Database(database_url) as db:
        async with db.transaction():
            await db.execute(
                'insert into users (name) values (:name)', {'name': 'root'}
            )
        assert await db.fetch_val('select count(*) from users') == 1
        await db.execute('delete from users')


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_transaction_rollback(database_url):
    """
    Given an empty database
    When I start transaction
    And insert one object
    And raise exception
    Then the transaction has to be rolled back
    And total count of rows must be zero.
    """
    async with Database(database_url) as db:
        with pytest.raises(Exception):
            async with db.transaction(force_rollback=True):
                await db.execute(
                    'insert into users (name) values (:name)', {'name': 'root'}
                )
                raise Exception()
        assert await db.fetch_val('select count(*) from users') == 0


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_nested_transactions(database_url):
    """
    Given empty database
    When I start transaction
    And insert one object
    And start another transaction
    And raise exception inside the second transaction
    Then the second transaction has to be rolled back
    And total count of rows must be equal to one.
    """

    async with Database(database_url) as db:
        async with db.transaction():
            await db.execute(
                'insert into users (name) values (:name)', {'name': 'root'}
            )

            with pytest.raises(Exception):
                async with db.transaction():
                    await db.execute(
                        'insert into users (name) values (:name)',
                        {'name': 'root'}
                    )
                    raise Exception()
        assert await db.fetch_val('select count(*) from users') == 1


class _SampleDriver:
    def __init__(self, *args, **kwargs):
        pass


def test_create_driver():
    Database.register_driver('dummy', 'tests.test_database._SampleDriver')
    db = Database('dummy://')
    driver = db.create_driver()
    assert isinstance(driver, _SampleDriver)
