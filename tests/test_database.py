import tempfile

import pytest

import aerie
from aerie import Database


@pytest.fixture()
def tmp_file():
    tmp_file = tempfile.NamedTemporaryFile(suffix='.db')
    yield tmp_file


@pytest.fixture()
def db(tmp_file):
    return aerie.Database('sqlite://%s' % tmp_file.name)


@pytest.fixture(autouse=True)
async def database(db):
    await db.execute(
        'create table if not exists users '
        '(id integer primary key, name text)'
    )
    yield
    await db.execute('drop table users')


@pytest.mark.asyncio
async def test_queries(db):
    # test execute and fetch_val
    await db.execute(
        'insert into users (name) values (:name)', {'name': 'root'}
    )
    assert await db.fetch_val(
        'select count(*) from users where name = "root"'
    ) == 1

    # test execute_all, fetch_all and fetch_one
    await db.execute_all(
        'insert into users (name) values (:name)',
        [{'name': 'user1'}, {'name': 'user2'}]
    )
    rows = await db.fetch_all(
        'select * from users where name in ("user1", "user2")'
    )
    assert rows[0]['name'] == 'user1'
    assert rows[1]['name'] == 'user2'

    row = await db.fetch_one(
        'select * from users where name in ("user1", "user2") limit 1'
    )
    assert row['name'] == 'user1'

    # test iterate
    iterator = db.iterate('select * from users')
    row = await iterator.__anext__()
    assert dict(row) == dict(id=1, name='root')

    row = await iterator.__anext__()
    assert dict(row) == dict(id=2, name='user1')


@pytest.mark.asyncio
async def test_transaction_commit(db):
    """
    Given an empty database
    When I start transaction
    And insert one object
    And no exception raised
    Then the transaction has to be commited
    And total count of rows must be equal to 1.
    """
    async with db.transaction():
        await db.execute(
            'insert into users (name) values (:name)', {'name': 'root'}
        )
    assert await db.fetch_val('select count(*) from users') == 1


@pytest.mark.asyncio
async def test_transaction_rollback(db):
    """
    Given an empty database
    When I start transaction
    And insert one object
    And raise exception
    Then the transaction has to be rolled back
    And total count of rows must be zero.
    """
    with pytest.raises(Exception):
        async with db.transaction():
            await db.execute(
                'insert into users (name) values (:name)', {'name': 'root'}
            )
            raise Exception()
    assert await db.fetch_val('select count(*) from users') == 0


@pytest.mark.asyncio
async def test_nested_transactions(db):
    """
    Given empty database
    When I start transaction
    And insert one object
    And start another transaction
    And raise exception inside the second transaction
    Then the second transaction has to be rolled back
    And total count of rows must be equal to one.
    """

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
