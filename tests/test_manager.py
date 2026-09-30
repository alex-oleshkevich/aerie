import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.pool import NullPool

from aerie.manager import DatabaseManager


@pytest.fixture
def dm(database_url: str) -> DatabaseManager:
    return DatabaseManager(database_url, pool_size=1, max_overflow=0)


class TestDatabaseManagerContextManager:
    """Test DatabaseManager async context manager behavior."""

    async def test_context_manager_creates_engine(self, dm: DatabaseManager) -> None:
        """Test that entering context manager creates engine."""
        assert dm._engine is None

        async with dm:
            assert isinstance(dm.get_engine(), AsyncEngine)

    async def test_context_manager_reuses_engine(self, dm: DatabaseManager) -> None:
        """Test that multiple entries reuse the same engine."""
        async with dm as engine1, dm as engine2:
            assert engine1 is engine2

    async def test_get_engine_before_initialization(self, dm: DatabaseManager) -> None:
        """Test get_engine raises error when not initialized."""
        with pytest.raises(LookupError, match="No SQLAlchemy engine is running"):
            dm.get_engine()

    async def test_pool_configuration_disabled(self, database_url: str) -> None:
        """Test pool configuration when dangerously_disable_pool=True."""
        database_manager = DatabaseManager(
            database_url,
            dangerously_disable_pool=True,
            pool_size=10,
        )

        async with database_manager as manager:
            assert isinstance(manager.get_engine().pool, NullPool)


class TestDatabaseManagerSessions:
    """Test DatabaseManager session management."""

    async def test_normal_session_creation(self, dm: DatabaseManager) -> None:
        """Test normal sessions commit data permanently."""

        async with dm:
            async with dm.session() as session:
                await session.execute(sa.text("CREATE TEMPORARY TABLE test (test_data VARCHAR(50))"))
                await session.execute(sa.text("INSERT INTO test (test_data) VALUES ('value')"))
                await session.commit()

            async with dm.session() as new_session:
                result = await new_session.execute(sa.text("SELECT COUNT(*) FROM test"))
                count = result.scalar()
                assert count == 1

    async def test_rollback_session_creation(self, dm: DatabaseManager) -> None:
        """Test rollback sessions automatically rollback data."""

        async with dm:
            async with dm.get_engine().begin() as tx:
                await tx.execute(sa.text("CREATE TEMPORARY TABLE test (test_data VARCHAR(50))"))

            async with dm.session(force_rollback=True) as session:
                await session.execute(sa.text("INSERT INTO test (test_data) VALUES ('value')"))
                await session.commit()

            async with dm.session() as new_session:
                result = await new_session.execute(sa.text("SELECT COUNT(*) FROM test"))
                count = result.scalar()
                assert count == 0

    async def test_session_without_initialized_engine(self, dm: DatabaseManager) -> None:
        with pytest.raises(AssertionError, match="SQLAlchemy engine is not initialized"):
            async with dm.session():
                pass

    async def test_nested_sessions_are_independent(self, dm: DatabaseManager) -> None:
        async with dm, dm.session() as outer, dm.session() as inner:
            assert inner is not outer


class TestManagerSurface:
    def test_dsn_is_exposed(self, database_url: str) -> None:
        assert DatabaseManager(database_url).dsn == database_url

    async def test_exit_without_enter_is_a_no_op(self, database_url: str) -> None:
        manager = DatabaseManager(database_url)

        await manager.__aexit__(None, None, None)

        assert manager._engine is None

    async def test_normal_session_requires_an_engine(self, database_url: str) -> None:
        manager = DatabaseManager(database_url)

        with pytest.raises(RuntimeError, match="not initialized"):
            async with manager._start_normal_session():
                pass

    async def test_rollback_session_requires_an_engine(self, database_url: str) -> None:
        manager = DatabaseManager(database_url)

        with pytest.raises(RuntimeError, match="not initialized"):
            async with manager._start_rollback_session():
                pass


class TestManagerLifecycle:
    async def test_exiting_deactivates_the_manager(self, database_url: str) -> None:
        manager = DatabaseManager(database_url)
        async with manager:
            assert manager.get_engine() is not None

        with pytest.raises(LookupError, match="No SQLAlchemy engine is running"):
            manager.get_engine()


class TestManagerNesting:
    async def test_inner_exit_does_not_dispose_the_shared_engine(self, database_url: str) -> None:
        manager = DatabaseManager(database_url)
        async with manager:
            async with manager:
                pass
            async with manager.session() as session:
                assert session is not None


class TestSessionReuse:
    async def test_a_nested_call_joins_the_open_session(self, database_url: str) -> None:
        async with (
            DatabaseManager(database_url, force_session_reuse=True) as manager,
            manager.session() as outer,
            manager.session() as inner,
        ):
            assert inner is outer

    async def test_without_reuse_a_nested_call_starts_its_own(self, database_url: str) -> None:
        async with (
            DatabaseManager(database_url) as manager,
            manager.session() as outer,
            manager.session() as inner,
        ):
            assert inner is not outer

    async def test_no_session_is_current_outside_the_context(self, database_url: str) -> None:
        async with DatabaseManager(database_url, force_session_reuse=True) as manager:
            assert manager.current_session() is None
            async with manager.session() as session:
                assert manager.current_session() is session
            assert manager.current_session() is None
