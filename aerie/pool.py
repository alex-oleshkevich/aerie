import asyncio
import typing as t
from contextlib import asynccontextmanager

from aerie.protocols import Connection, Driver


class Pool:
    def __init__(self, driver: Driver, max_size: int = 10):
        self.driver = driver
        self.max_size = max_size
        self._connections: t.List[Connection] = []
        self._busy_connections: t.List[Connection] = []

    @asynccontextmanager
    async def acquire(self) -> t.AsyncContextManager[Connection]:
        if len(self._connections):
            connection = self._connections.pop()
        else:
            if len(self) < self.max_size:
                connection = await self.driver.create_connection()
            else:
                while not len(self._connections):
                    await asyncio.sleep(0.0001)
                connection = self._connections.pop()

        self._busy_connections.append(connection)
        yield connection
        self._busy_connections.remove(connection)
        self._connections.append(connection)

    def __len__(self) -> int:
        return len(self._connections) + len(self._busy_connections)
