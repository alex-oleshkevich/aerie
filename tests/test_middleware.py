import typing

import anyio
import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from aerie.ext.starlette import DbSessionMiddleware
from aerie.manager import DatabaseManager


class SimpleASGIApp:
    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
            await send({"type": "http.response.body", "body": b"OK"})


@pytest.fixture
def simple_app() -> SimpleASGIApp:
    """Create a simple ASGI app for testing."""
    return SimpleASGIApp()


@pytest.fixture
async def database(database_url: str) -> typing.AsyncGenerator[DatabaseManager]:
    async with DatabaseManager(database_url, pool_size=1, max_overflow=0) as manager:
        yield manager


class TestDbSessionMiddleware:
    @pytest.mark.parametrize("type", ("http", "websocket"))
    async def test_middleware_adds_dbsession_to_requests(
        self, type: str, simple_app: ASGIApp, database: DatabaseManager, dbsession: AsyncSession
    ) -> None:
        scope: Scope = {"type": type, "method": "GET", "path": "/test", "state": {}}

        async def receive() -> Message:
            return {"type": "http.request", "body": b""}

        async def send(message: Message) -> None:
            pass

        middleware = DbSessionMiddleware(simple_app, database)
        await middleware(scope, receive, send)
        assert isinstance(scope["state"]["dbsession"], AsyncSession)

    async def test_middleware_skips_non_http_websocket_requests(
        self, database: DatabaseManager, simple_app: SimpleASGIApp, dbsession: AsyncSession
    ) -> None:
        scope = {"type": "lifespan", "asgi": {"version": "3.0"}, "state": {}}

        async def receive() -> Message:
            return {"type": "lifespan.startup"}

        async def send(message: Message) -> None:
            pass

        middleware = DbSessionMiddleware(simple_app, database)
        await middleware(scope, receive, send)
        assert "dbsession" not in scope["state"]

    async def test_middleware_creates_separate_sessions_for_concurrent_requests(
        self, simple_app: SimpleASGIApp, database: DatabaseManager, dbsession: AsyncSession
    ) -> None:
        scope: Scope = {"type": "http", "method": "GET", "path": "/test", "state": {}}
        scope2: Scope = {"type": "http", "method": "GET", "path": "/test", "state": {}}

        async def make_request(scope: Scope) -> None:
            async def receive() -> Message:
                return {"type": "http.request", "body": b""}

            async def send(message: Message) -> None:
                pass

            middleware = DbSessionMiddleware(simple_app, database)
            await middleware(scope, receive, send)

        async with anyio.create_task_group() as tg:
            tg.start_soon(make_request, scope)
            tg.start_soon(make_request, scope2)

        assert scope["state"]["dbsession"] and scope2["state"]["dbsession"]
        assert scope["state"]["dbsession"] != scope2["state"]["dbsession"]
