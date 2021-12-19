import asyncio
import os
import pytest
import typing as t

from aerie import Aerie
from tests.tables import address_table, metadata, profile_table, user_to_address, users_table

DATABASE_URLS = [
    'sqlite+aiosqlite:///:memory:',
    os.environ.get('POSTGRES_URL', 'postgresql+asyncpg://postgres:postgres@localhost/aerie'),
]

databases = [Aerie(url, metadata=metadata) for url in DATABASE_URLS]


@pytest.fixture(scope='session')
def event_loop() -> asyncio.AbstractEventLoop:
    return asyncio.get_event_loop()


@pytest.fixture()
def db() -> Aerie:
    return databases[0]


@pytest.fixture(autouse=True, scope='session')
@pytest.mark.asyncio
async def create_tables() -> t.AsyncGenerator[None, None]:
    for db in databases:
        await db.schema.drop_tables()
        await db.schema.create_tables()
        await db.execute(
            users_table.insert(
                [
                    {'id': 1, 'name': 'User One'},
                    {'id': 2, 'name': 'User Two'},
                    {'id': 3, 'name': 'User Three'},
                ]
            )
        )
        await db.execute(
            profile_table.insert(
                [
                    {'id': 1, 'first_name': 'User', 'last_name': 'One', 'user_id': 1},
                    {'id': 2, 'first_name': 'User', 'last_name': 'Two', 'user_id': 2},
                ]
            )
        )
        await db.execute(
            address_table.insert(
                [
                    {'id': 1, 'city': 'Minsk', 'street': 'Skaryny pr.'},
                    {'id': 2, 'city': 'Stoubcy', 'street': 'Central str.'},
                ]
            )
        )
        await db.execute(
            user_to_address.insert(
                [
                    {'user_id': 1, 'address_id': 1},
                    {'user_id': 2, 'address_id': 2},
                ]
            )
        )
    yield
    # for db in databases:
    #     await db.schema.drop_tables()
