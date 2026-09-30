"""Fixtures for the database suite.

Requires a reachable PostgreSQL instance. Point `DATABASE_URL` at it; the default
matches the docker-compose service most projects run locally.

Every test gets a rollback session: writes are visible inside the test and undone
on teardown, so the tables are created once per session and never truncated.
"""

import os
import typing

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase

from aerie.manager import DatabaseManager
from aerie.models import Base
from aerie.mutation import DeleteQuery, ReturningQuery, UpdateQuery
from aerie.querying import Query, ScalarQuery
from aerie.querying import query as open_query
from tests.models import Author, Counter, Draft, Post, Tag

DEFAULT_DATABASE_URL = "postgresql+psycopg://postgres:postgres@localhost:5432/testdb"


def pytest_report_header() -> list[str]:
    return [f"database: {os.environ.get('DATABASE_URL', DEFAULT_DATABASE_URL)}"]


@pytest.fixture(scope="session")
def database_url() -> str:
    """DSN of the database the suite runs against."""
    return os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)


@pytest.fixture(scope="session")
async def database_manager(database_url: str) -> typing.AsyncGenerator[DatabaseManager]:
    """A manager with the fixture tables created, shared by the whole session."""
    tables = [
        Base.metadata.tables[Author.__tablename__],
        Base.metadata.tables[Post.__tablename__],
        Base.metadata.tables[Tag.__tablename__],
        Base.metadata.tables[Counter.__tablename__],
        Base.metadata.tables[Draft.__tablename__],
    ]
    async with DatabaseManager(database_url, pool_size=5, max_overflow=0) as manager:
        async with manager.get_engine().begin() as connection:
            await connection.run_sync(Base.metadata.drop_all, tables=tables)
            await connection.run_sync(Base.metadata.create_all, tables=tables)
        yield manager


@pytest.fixture
async def dbsession(database_manager: DatabaseManager) -> typing.AsyncGenerator[AsyncSession]:
    """A session whose writes are rolled back when the test ends."""
    async with database_manager.session(force_rollback=True) as dbsession:
        yield dbsession


@pytest.fixture
async def author(dbsession: AsyncSession) -> Author:
    """The author that owns most of the seeded posts."""
    author = Author(name="alice", blog_id=1)
    dbsession.add(author)
    await dbsession.flush()
    return author


@pytest.fixture
async def other_author(dbsession: AsyncSession) -> Author:
    """An author on a different blog, so filters have something to exclude."""
    author = Author(name="bob", blog_id=2)
    dbsession.add(author)
    await dbsession.flush()
    return author


@pytest.fixture
async def seeded_posts(dbsession: AsyncSession, author: Author, other_author: Author) -> tuple[Post, Post, Post]:
    """Three posts: two on the author's blog, one on another."""
    alpha = Post(title="alpha-post", blog_id=author.blog_id, author_id=author.id)
    beta = Post(title="beta-post", blog_id=author.blog_id, author_id=author.id)
    gamma = Post(title="gamma-post", blog_id=other_author.blog_id, author_id=other_author.id)
    dbsession.add_all([alpha, beta, gamma])
    await dbsession.flush()
    return alpha, beta, gamma


class BoundQuery:
    """Test shorthand that supplies the fixture session to every terminal."""

    _terminals = {
        "all",
        "avg",
        "batches",
        "batches_by",
        "count",
        "cursor_page",
        "execute",
        "exists",
        "first",
        "first_or_raise",
        "iter",
        "iter_by",
        "map",
        "max",
        "min",
        "one",
        "one_or_none",
        "one_or_raise",
        "paginate",
        "set",
        "sum",
    }
    _query_types = (Query, UpdateQuery, DeleteQuery, ReturningQuery)

    def __init__(self, target: typing.Any, session: AsyncSession) -> None:
        self._target = target
        self._session = session

    def __getattr__(self, name: str) -> typing.Any:
        attribute = getattr(self._target, name)
        if name in self._terminals and (name != "set" or isinstance(self._target, ScalarQuery)):
            return lambda *args, **kwargs: attribute(self._session, *args, **kwargs)
        if not callable(attribute):
            return attribute

        def call(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            result = attribute(*args, **kwargs)
            if isinstance(result, self._query_types):
                return BoundQuery(result, self._session)
            return result

        return call


class OpenQuery(typing.Protocol):
    """A `query` with its test session supplied by the fixture."""

    def __call__(self, model: type[DeclarativeBase]) -> typing.Any: ...


@pytest.fixture
def query(dbsession: AsyncSession) -> OpenQuery:
    """`query` shorthand that supplies the test's rollback session to terminals."""

    def open(model: type[DeclarativeBase]) -> typing.Any:
        return BoundQuery(open_query(model), dbsession)

    return open
