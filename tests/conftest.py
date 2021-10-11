import asyncio
import os
import pytest
import sqlalchemy as sa
import typing as t

from aerie import Aerie
from aerie.models import Model

DATABASE_URLS = [
    'sqlite+aiosqlite:///:memory:',
    os.environ.get('POSTGRES_URL', 'postgresql+asyncpg://postgres:postgres@localhost/aerie'),
]

databases = [Aerie(url) for url in DATABASE_URLS]

metadata = sa.MetaData()
users = sa.Table(
    'users',
    metadata,
    sa.Column(sa.Integer, name='id', primary_key=True),
    sa.Column(sa.String, name='name', nullable=True),
)


class User(Model):
    __tablename__ = 'users'
    id = sa.Column(sa.BigInteger, primary_key=True)
    name = sa.Column(sa.String)


@pytest.fixture(scope='session')
def event_loop() -> asyncio.AbstractEventLoop:
    return asyncio.get_event_loop()


@pytest.fixture(autouse=True, scope='session')
@pytest.mark.asyncio
async def create_tables() -> t.AsyncGenerator[None, None]:
    for db in databases:
        await db.drop_tables()
        await db.create_tables()
        await db.query(
            users.insert(
                [
                    {'id': 1, 'name': 'User One'},
                    {'id': 2, 'name': 'User Two'},
                    {'id': 3, 'name': 'User Three'},
                ]
            )
        ).execute()
    yield
    for db in databases:
        await db.drop_tables()
