import contextlib
import contextvars
import typing
from types import TracebackType

from sqlalchemy import NullPool
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


class DatabaseManager:
    def __init__(
        self,
        url: str,
        *,
        pool_pre_ping: bool = False,
        pool_size: int = 15,
        pool_recycle: int = 1000,
        pool_timeout: int = 10,
        max_overflow: int = 2,
        echo: bool = False,
        isolation_level: str = "READ COMMITTED",
        dangerously_disable_pool: bool = False,
        force_session_reuse: bool = False,
    ) -> None:
        self._url = url
        self._echo = echo
        self._pool_size = pool_size
        self._pool_pre_ping = pool_pre_ping
        self._pool_recycle = pool_recycle
        self._pool_timeout = pool_timeout
        self._max_overflow = max_overflow
        self._dangerously_disable_pool = dangerously_disable_pool
        self._isolation_level = isolation_level
        self._force_session_reuse = force_session_reuse
        self._current_session: contextvars.ContextVar[AsyncSession | None] = contextvars.ContextVar(
            "_current_session", default=None
        )
        self._engine: AsyncEngine | None = None
        self._sessionmaker: async_sessionmaker[AsyncSession] | None = None
        self._depth = 0

    @property
    def dsn(self) -> str:
        return self._url

    async def __aenter__(self) -> typing.Self:
        # Reference-counted: a nested `async with` on the same manager must not let
        # the inner exit dispose an engine the outer scope is still using.
        self._depth += 1
        if self._engine is not None:
            return self

        opts: dict[str, typing.Any] = {
            "pool_size": self._pool_size,
            "pool_recycle": self._pool_recycle,
            "pool_timeout": self._pool_timeout,
            "max_overflow": self._max_overflow,
        }
        if self._dangerously_disable_pool:
            opts = {"poolclass": NullPool}

        self._engine = create_async_engine(
            self._url,
            echo=self._echo,
            pool_pre_ping=self._pool_pre_ping,
            isolation_level=self._isolation_level,
            **opts,
        )
        self._sessionmaker = async_sessionmaker(bind=self._engine, expire_on_commit=False)
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        self._depth = max(0, self._depth - 1)
        if self._depth == 0 and self._engine is not None:
            await self._engine.dispose()
            # Clear both, or get_engine() keeps handing out a disposed engine
            # instead of raising the LookupError its message promises.
            self._engine = None
            self._sessionmaker = None

    def get_engine(self) -> AsyncEngine:
        if not self._engine:
            raise LookupError(
                "No SQLAlchemy engine is running. Use DatabaseManager as a context manager to activate it."
            )
        return self._engine

    def current_session(self) -> AsyncSession | None:
        """The session open in this context, if any. For session reuse; not for general use."""
        return self._current_session.get()

    @contextlib.asynccontextmanager
    async def session(self, force_rollback: bool = False) -> typing.AsyncGenerator[AsyncSession]:
        """Get a new session instance.
        If force_rollback is True, the session will be rolled back after exiting the context."""
        assert self._sessionmaker is not None, "SQLAlchemy engine is not initialized."

        # With reuse on, a nested caller joins the session already open in this context
        # instead of starting its own. A test can then hand one rollback session to the
        # whole call tree — middleware, factories, the code under test — without any of
        # them being told about it.
        if self._force_session_reuse and (current := self.current_session()) is not None:
            yield current
            return

        starter: typing.Callable[[], typing.AsyncContextManager[AsyncSession]] = (
            self._start_rollback_session if force_rollback else self._start_normal_session
        )
        async with starter() as session:
            token = self._current_session.set(session)
            try:
                yield session
            finally:
                self._current_session.reset(token)

    @contextlib.asynccontextmanager
    async def _start_normal_session(self, **kwargs: typing.Any) -> typing.AsyncGenerator[AsyncSession]:
        if self._sessionmaker is None:
            raise RuntimeError("SQLAlchemy engine is not initialized.")

        async with self._sessionmaker(**kwargs) as session:
            yield session

    @contextlib.asynccontextmanager
    async def _start_rollback_session(self) -> typing.AsyncGenerator[AsyncSession]:
        """Create a new session that will be rolled back after exiting the context.
        For testing purposes. Any BEGIN/COMMIT/ROLLBACK ops are executed in SAVEPOINT.
        See https://docs.sqlalchemy.org/en/20/orm/session_transaction.html#joining-a-session-into-an-external-transaction-such-as-for-test-suites"""
        if self._engine is None:
            raise RuntimeError("SQLAlchemy engine is not initialized.")

        async with self._engine.connect() as conn, conn.begin() as tx:  # noqa: SIM117 - keep the rollback nesting explicit
            async with self._start_normal_session(bind=conn, join_transaction_mode="create_savepoint") as session:
                try:
                    yield session
                finally:
                    await tx.rollback()
