from starlette.types import ASGIApp, Receive, Scope, Send

from aerie.manager import DatabaseManager


class DbSessionMiddleware:
    def __init__(self, app: ASGIApp, database: DatabaseManager) -> None:
        self.app = app
        self.database = database

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ["http", "websocket"]:
            return await self.app(scope, receive, send)

        async with self.database.session() as dbsession:
            scope.setdefault("state", {})
            scope["state"]["dbsession"] = dbsession
            await self.app(scope, receive, send)
