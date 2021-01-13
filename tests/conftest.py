import asyncio
import os

import pytest

from aerie import Database
from aerie.schema import types
from aerie.url import URL

DATABASES = [
    URL(
        os.environ.get('SQLITE_URL', 'sqlite:///tmp/aerie.test.db')
    ),
    URL(
        os.environ.get(
            'POSTGRES_URL',
            'postgresql://postgres:postgres@localhost:5432/aerie_test'
        )
    ),
]


@pytest.yield_fixture(scope='module')
def event_loop(request):
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


async def create_database(database_url: URL):
    async with Database(database_url) as db:
        editor = db.schema_editor()
        with editor.create_table('users', if_not_exists=True) as table:
            table.increments()
            table.add_column('name', types.String())
            table.add_index(
                'name', name='name_uniq', unique=True, if_not_exists=True,
            )
        with editor.create_table('books', if_not_exists=True) as table:
            table.increments()
            table.add_column('name', types.String())
            table.add_column('author', types.String())

        await editor.apply()


async def drop_database(database_url: URL):
    async with Database(database_url) as db:
        editor = db.schema_editor()
        editor.drop_table_if_exists('users')
        editor.drop_table_if_exists('books')
        await editor.apply()


@pytest.fixture(autouse=True, scope='module')
@pytest.mark.asyncio
async def setup_databases():
    for database_url in DATABASES:
        await create_database(database_url)
    yield
    for database_url in DATABASES:
        await drop_database(database_url)
