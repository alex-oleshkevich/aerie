import asyncio
from dataclasses import dataclass

import pytest

from aerie import Database
from aerie.exceptions import UniqueViolationError
from aerie.terms import OnConflict
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
        await connection.execute(
            'create unique index name_uniq on users (name)')
        await connection.close()
    elif database_url.driver == 'postgresql':
        import asyncpg
        connection = await asyncpg.connect(database_url.url)
        sql = (
            "create table if not exists public.users "
            "(id serial primary key, name varchar(256))"
        )
        await connection.execute(sql)
        await connection.execute(
            'create unique index name_uniq on users (name)')
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


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_fetch_one_with_default(database_url):
    async with Database(database_url) as db:
        async with db.transaction(force_rollback=True):
            assert await db.fetch_one(
                "select * from users", default='fallback'
            ) == 'fallback'


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
        async with db.transaction(force_rollback=True):
            async with db.transaction():
                await db.execute(
                    'insert into users (name) values (:name)', {'name': 'root'}
                )
            assert await db.fetch_val('select count(*) from users') == 1


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
        async with db.transaction(force_rollback=True):
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


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_transaction_manual_commit_control(database_url):
    async with Database(database_url) as db:
        async with db.transaction(force_rollback=True):
            tx = db.transaction()
            try:
                await tx.begin()
                await db.execute('insert into users (name) values (:name)', {
                    'name': 'transaction_commit_manual'
                })
                await tx.commit()
            except:  # pragma: nocover
                await tx.rollback()

            assert await db.fetch_one(
                'select * from users where name = :name', {
                    'name': 'transaction_commit_manual'
                })


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_transaction_manual_rollback_control(database_url):
    async with Database(database_url) as db:
        async with db.transaction(force_rollback=True):
            tx = db.transaction()
            try:
                await tx.begin()
                await db.execute('insert into users (name) values (:name)', {
                    'name': 'transaction_rollback_manual'
                })
                await tx.rollback()
            except:  # pragma: nocover
                await tx.rollback()

            assert not await db.fetch_one(
                'select * from users where name = :name', {
                    'name': 'transaction_rollback_manual'
                })


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_transaction_awaitable_transaction(database_url):
    """Transaction must begin transaction when awaiting the transaction object."""
    async with Database(database_url) as db:
        async with db.transaction(force_rollback=True):
            tx = await db.transaction()
            try:
                await db.execute('insert into users (name) values (:name)', {
                    'name': 'transaction_await_manual'
                })
                await tx.rollback()
            except:  # pragma: nocover
                await tx.rollback()

            assert not await db.fetch_one(
                'select * from users where name = :name', {
                    'name': 'transaction_await_manual'
                })


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_transaction_rollbacks_forcibly(database_url):
    """It must always rollback the transaction if `force_rollback` is True."""
    async with Database(database_url) as db:
        async with db.transaction(force_rollback=True):
            async with db.transaction(force_rollback=True):
                await db.execute('insert into users (name) values (:name)', {
                    'name': 'transaction_force_rollback'
                })

            assert not await db.fetch_one(
                'select * from users where name = :name', {
                    'name': 'transaction_force_rollback'
                })


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_fetch_one_returns_none(database_url):
    async with Database(database_url) as db:
        async with db.transaction(force_rollback=True):
            assert await db.fetch_one(
                'select * from users where id = -1'
            ) is None


class _SampleDriver:
    def __init__(self, *args, **kwargs):
        pass


def test_create_driver():
    Database.register_driver('dummy', 'tests.test_database._SampleDriver')
    db = Database('dummy://')
    driver = db.create_driver()
    assert isinstance(driver, _SampleDriver)


@dataclass
class User:
    id: int
    name: str


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_iterate_maps_to_callable(database_url):
    async with Database(database_url) as db:
        async with db.transaction(force_rollback=True):
            await db.execute_all(
                'insert into users (name) values (:name)',
                [
                    {'name': 'map1'},
                    {'name': 'map2'}
                ]
            )

            row = await db.fetch_one(
                'select * from users where name = :name', {'name': 'map1'},
                entity_factory=lambda row: User(**row),
            )
            assert isinstance(row, User)
            assert row.name == 'map1'

            rows = await db.fetch_all(
                'select * from users',
                entity_factory=lambda row: User(**row),
            )
            assert all([isinstance(row, User) for row in rows])

            rows = []
            async for row in db.iterate(
                    'select * from users',
                    entity_factory=lambda row: User(**row),
            ):
                rows.append(row)
            assert all([isinstance(row, User) for row in rows])


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_iterate_maps_to_class(database_url):
    async with Database(database_url) as db:
        async with db.transaction(force_rollback=True):
            await db.execute_all(
                'insert into users (name) values (:name)',
                [
                    {'name': 'map1'},
                    {'name': 'map2'}
                ]
            )

            row = await db.fetch_one(
                'select * from users where name = :name', {'name': 'map1'},
                map_to=User,
            )
            assert isinstance(row, User)
            assert row.name == 'map1'

            rows = await db.fetch_all('select * from users', map_to=User)
            assert all([isinstance(row, User) for row in rows])

            rows = []
            async for row in db.iterate(
                    'select * from users', map_to=User
            ):
                rows.append(row)
            assert all([isinstance(row, User) for row in rows])


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_iterate_cant_map_with_factory_and_class(database_url):
    async with Database(database_url) as db:
        async with db.transaction(force_rollback=True):
            await db.execute_all(
                'insert into users (name) values (:name)',
                [
                    {'name': 'map1'},
                    {'name': 'map2'}
                ]
            )

            with pytest.raises(AssertionError):
                await db.fetch_one(
                    'select * from users',
                    map_to=User,
                    entity_factory=lambda row: row,
                )

            with pytest.raises(AssertionError):
                await db.fetch_all(
                    'select * from users',
                    map_to=User,
                    entity_factory=lambda row: row,
                )

            with pytest.raises(AssertionError):
                async for row in db.iterate(
                        'select * from users',
                        map_to=User,
                        entity_factory=lambda row: row
                ):
                    pass


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_insert(database_url):
    async with Database(database_url) as db:
        async with db.transaction(force_rollback=True):
            await db.insert('users', {'name': 'user1'})
            await db.insert('users', {'name': 'user2'})

            rows = await db.fetch_all('select name from users')
            assert rows.pluck('name') == ['user1', 'user2']


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_insert_all(database_url):
    async with Database(database_url) as db:
        async with db.transaction(force_rollback=True):
            await db.insert_all('users', [
                {'name': 'user1'},
                {'name': 'user2'},
            ])

            rows = await db.fetch_all('select name from users')
            assert rows.pluck('name') == ['user1', 'user2']


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_upsert(database_url):
    async with Database(database_url) as db:
        async with db.transaction(force_rollback=True):
            await db.insert('users', {'name': 'user1'})
            await db.insert(
                'users', {'name': 'user1'},
                on_conflict=OnConflict.NOTHING,
            )

            # unique violation by ID field
            # we replace name
            await db.insert(
                'users', {'name': 'user2', 'id': 1},
                on_conflict=OnConflict.REPLACE,
                conflict_target=['id'],
            )
            assert await db.count(
                'users', where='name = :name', where_params={'name': 'user2'}
            ) == 1

            # unique violation by ID field
            # we exclude name from being replaced
            await db.insert(
                'users', {'name': 'user3', 'id': 1},
                on_conflict=OnConflict.REPLACE,
                conflict_target=['id'],
                replace_except=['name'],
            )
            # update did not happen
            assert await db.count(
                'users', where='name = :name', where_params={'name': 'user2'}
            ) == 1
            assert await db.count(
                'users', where='name = :name', where_params={'name': 'user3'}
            ) == 0


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_unique_error_to_aerie_error(database_url):
    async with Database(database_url) as db:
        async with db.transaction(force_rollback=True):
            await db.insert('users', {'name': 'user1'})
            with pytest.raises(UniqueViolationError):
                await db.insert('users', {'name': 'user1'})


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_count(database_url):
    async with Database(database_url) as db:
        async with db.transaction(force_rollback=True):
            await db.insert('users', {'name': 'user1'})

            assert await db.count('users') == 1
            assert await db.count('users', column='name') == 1
            assert await db.count('users', where='name = :name', where_params={
                'name': 'user1',
            }) == 1
            assert await db.count('users', where='name = :name', where_params={
                'name': 'user2',
            }) == 0
            users = db.query_builder().Table('users')
            assert await db.count('users', where=users.name == 'user1') == 1


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_update(database_url):
    async with Database(database_url) as db:
        async with db.transaction(force_rollback=True):
            await db.insert('users', {'name': 'user1'})

            # no where
            await db.update('users', {'id': 10, 'name': 'newname'})
            assert await db.count(
                'users', where='name = :name', where_params={'name': 'newname'}
            ) == 1

            # where as string
            await db.update(
                'users',
                set={'name': 'name2'},
                where='name = :wh_name',
                where_params={'wh_name': 'newname'},
            )
            assert await db.count(
                'users', where='name = :name', where_params={'name': 'name2'}
            ) == 1

            # where as pika term
            users = db.query_builder().Table('users')
            await db.update(
                'users',
                set={'name': 'name3'},
                where=users.name == 'name2',
            )
            assert await db.count(
                'users', where='name = :name', where_params={'name': 'name3'}
            ) == 1


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_delete(database_url):
    async with Database(database_url) as db:
        async with db.transaction(force_rollback=True):
            await db.insert_all('users', [
                {'name': 'user1'},
                {'name': 'user2'},
                {'name': 'user3'},
            ])

            # where as string
            await db.delete(
                'users',
                where='name = :name',
                where_params={'name': 'user1'},
            )
            assert await db.count('users') == 2

            # where as pika term
            users = db.query_builder().Table('users')
            await db.delete('users', where=users.name == 'user2')
            assert await db.count('users') == 1

            # no where
            await db.delete('users')
            assert await db.count('users') == 0
