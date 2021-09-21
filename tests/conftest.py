import os

import pytest

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from aerie.models import Model

DATABASE_URLS = [
    'sqlite+aiosqlite:///database.sqlite',
    os.environ.get('POSTGRES_URL', 'postgresql+asyncpg://postgres:postgres@localhost/aerie'),
]

metadata = sa.MetaData()
users = sa.Table(
    'users', metadata,
    sa.Column(sa.Integer, name='id', primary_key=True),
    sa.Column(sa.String, name='name', nullable=True),
)


class User(Model):
    __tablename__ = 'users'
    id = sa.Column(sa.BigInteger, primary_key=True)
    name = sa.Column(sa.String)


@pytest.fixture()
@pytest.mark.asyncio
async def create_tables():
    for url in DATABASE_URLS:
        engine = create_async_engine(url)
        async with engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)
            await conn.run_sync(metadata.create_all)


@pytest.fixture(autouse=True)
@pytest.mark.asyncio
async def fill_sample_data(create_tables):
    for url in DATABASE_URLS:
        engine = create_async_engine(url)
        async with engine.begin() as conn:
            await conn.execute(
                users.insert([
                    {'id': 1, 'name': 'User One'},
                    {'id': 2, 'name': 'User Two'},
                    {'id': 3, 'name': 'User Three'},
                ])
            )
    yield
    for url in DATABASE_URLS:
        engine = create_async_engine(url)
        async with engine.begin() as conn:
            await conn.execute(users.delete())
