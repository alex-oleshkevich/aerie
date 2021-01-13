import pytest

from aerie import Database
from aerie.exceptions import OperationError
from aerie.schema import types
from tests.conftest import DATABASES


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_databases(database_url):
    if database_url.driver == 'sqlite':
        with pytest.raises(OperationError):
            async with Database(database_url) as db:
                await db.schema_editor().list_databases()
        return

    async with Database(database_url) as db:
        editor = db.schema_editor()
        async with db.transaction(force_rollback=True):
            assert await editor.has_database(database_url.db_name)


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_sequences(database_url):
    if database_url.driver == 'sqlite':
        with pytest.raises(OperationError):
            async with Database(database_url) as db:
                await db.schema_editor().list_sequences()
        return

    async with Database(database_url) as db:
        editor = db.schema_editor()
        async with db.transaction(force_rollback=True):
            editor.create_sequence('test_seq')
            await editor.apply()
            assert await editor.has_sequence('test_seq')

            editor.clear()
            editor.rename_sequence('test_seq', 'new_test_seq')
            await editor.apply()
            assert not await editor.has_sequence('test_seq')
            assert await editor.has_sequence('new_test_seq')

            editor.clear()
            editor.alter_sequence(
                'new_test_seq', min_value=100, start_with=101, restart_with=101
            )
            await editor.apply()

            editor.clear()
            editor.drop_sequence('new_test_seq')
            await editor.apply()
            assert not await editor.has_sequence('new_test_seq')


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_views(database_url):
    async with Database(database_url) as db:
        editor = db.schema_editor()
        async with db.transaction(force_rollback=True):
            editor.create_view(
                'my_view', 'select 1 as name',
                replace=True, columns=['alter_name'],
            )
            await editor.apply()
            assert await editor.has_view('my_view')

            if database_url.driver == 'postgres':
                editor.rename_view('my_view', 'new_my_view', if_exists=True)
                await editor.apply()
                assert await editor.has_view('new_my_view')

                editor.rename_view('new_my_view', 'my_view', if_exists=True)
                await editor.apply()

            editor.clear()
            editor.drop_view('my_view')


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_tables(database_url):
    async with Database(database_url) as db:
        editor = db.schema_editor()
        async with db.transaction(force_rollback=True):
            with editor.create_table('table_name') as table:
                table.add_column('int', types.Integer(), primary_key=True)
            await editor.apply()
            assert await editor.has_table('table_name')

            editor.rename_table('table_name', 'new_table_name')
            await editor.apply()
            assert await editor.has_table('new_table_name')
            assert not await editor.has_table('table_name')

            editor.drop_table('new_table_name')
            await editor.apply()
            assert not await editor.has_table('new_table_name')


@pytest.mark.parametrize("database_url", DATABASES)
@pytest.mark.asyncio
async def test_columns(database_url):
    async with Database(database_url) as db:
        editor = db.schema_editor()
        async with db.transaction(force_rollback=True):
            with editor.create_table('table_name') as table:
                table.integer('one')
                table.integer('two')
            await editor.apply()
            assert await editor.has_column('one', 'table_name')

            with editor.alter_table('table_name') as table:
                table.drop_column('two')
            await editor.apply()
            assert not await editor.has_column('two', 'table_name')
