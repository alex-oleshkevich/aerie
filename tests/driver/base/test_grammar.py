import pypika
import pytest

from aerie.drivers.base import BaseGrammar
from aerie.schema import operations as ops, types
from aerie.schema.structure import Column


@pytest.fixture()
def grammar():
    return BaseGrammar()


# region CREATE TABLE
def test_get_create_table_sql(grammar):
    assert grammar.get_create_table_sql(
        ops.CreateTable(table_name='users', columns=[
            Column('id', types.Text()),
            Column('first_name', types.Text(), null=True),
            Column('last_name', types.Text(), null=False, default='Root'),
        ])
    ) == (
               "CREATE TABLE users "
               "(id TEXT, first_name TEXT NULL, last_name TEXT NOT NULL DEFAULT 'Root')"
           )

    assert grammar.get_create_table_sql(
        ops.CreateTable(table_name='users', columns=[
            Column('id', types.Text()),
            Column('first_name', types.Text(), null=True),
        ], temporary=True)
    ) == (
               "CREATE TEMPORARY TABLE users (id TEXT, first_name TEXT NULL)"
           )

    assert grammar.get_create_table_sql(
        ops.CreateTable(table_name='users', columns=[
            Column('id', types.Text()),
            Column('first_name', types.Text(), null=True),
        ], temporary=True, if_not_exist=True)
    ) == (
               "CREATE TEMPORARY TABLE IF NOT EXISTS users "
               "(id TEXT, first_name TEXT NULL)"
           )

    assert grammar.get_create_table_sql(
        ops.CreateTable(table_name='users', columns=[
            Column('id', types.Text()),
            Column('first_name', types.Text(), null=True),
        ], temporary=True, if_not_exist=True, primary_key='id')
    ) == (
               "CREATE TEMPORARY TABLE IF NOT EXISTS users "
               "(id TEXT, first_name TEXT NULL, PRIMARY KEY (id))"
           )


# endregion

# region RENAME TABLE
def test_get_rename_table_sql(grammar):
    assert grammar.get_rename_table_sql(
        ops.RenameTable('users', 'accounts')
    ) == "ALTER TABLE users RENAME TO accounts"


# endregion

# region DROP TABLE
def test_get_drop_table_sql(grammar):
    assert grammar.get_drop_table_sql(
        ops.DropTable('users')
    ) == "DROP TABLE users"

    assert grammar.get_drop_table_sql(
        ops.DropTable('users', if_exists=True)
    ) == "DROP TABLE IF EXISTS users"


# endregion

# region ADD COLUMN
def test_get_add_column_sql(grammar):
    assert grammar.get_add_column_sql(
        ops.AddColumn(
            table_name='users',
            column_name='email',
            column_type=types.String(128),
        )
    ) == "ALTER TABLE users ADD COLUMN email VARCHAR(128)"

    assert grammar.get_add_column_sql(
        ops.AddColumn(
            table_name='users',
            column_name='email',
            column_type=types.String(128),
            default='root@localhost',
        )
    ) == (
               "ALTER TABLE users ADD COLUMN "
               "email VARCHAR(128) DEFAULT 'root@localhost'"
           )

    assert grammar.get_add_column_sql(
        ops.AddColumn(
            table_name='users',
            column_name='email',
            column_type=types.String(128),
            default='root@localhost',
            null=True,
        )
    ) == (
               "ALTER TABLE users ADD COLUMN "
               "email VARCHAR(128) NULL DEFAULT 'root@localhost'"
           )

    assert grammar.get_add_column_sql(
        ops.AddColumn(
            table_name='users',
            column_name='email',
            column_type=types.String(128),
            default='root@localhost',
            null=True,
            if_not_exists=True,

        )
    ) == (
               "ALTER TABLE users ADD COLUMN IF NOT EXISTS "
               "email VARCHAR(128) NULL DEFAULT 'root@localhost'"
           )


# endregion

# region RENAME COLUMN
def test_get_rename_column_sql(grammar):
    assert grammar.get_rename_column_sql(
        ops.RenameColumn(
            table_name='users',
            column_name='email',
            new_column_name='old_email'
        )
    ) == "ALTER TABLE users RENAME COLUMN email TO old_email"

    assert grammar.get_rename_column_sql(
        ops.RenameColumn(
            table_name='users',
            column_name='email',
            new_column_name='old_email',
            if_exists=True,
        )
    ) == "ALTER TABLE users IF EXISTS RENAME COLUMN email TO old_email"


# endregion

# region ALTER COLUMN

# endregion

# region DROP COLUMN
def test_get_drop_column_sql(grammar):
    assert grammar.get_drop_column_sql(
        ops.DropColumn(
            table_name='users',
            column_name='email',
        )
    ) == "ALTER TABLE users DROP COLUMN email"

    assert grammar.get_drop_column_sql(
        ops.DropColumn(
            table_name='users',
            column_name='email',
            if_exists=True,
        )
    ) == "ALTER TABLE users DROP COLUMN IF EXISTS email"

    assert grammar.get_drop_column_sql(
        ops.DropColumn(
            table_name='users',
            column_name='email',
            if_exists=True,
            cascade=True,
        )
    ) == "ALTER TABLE users DROP COLUMN IF EXISTS email CASCADE"


# endregion

# region CREATE INDEX
def test_get_create_index_sql(grammar):
    assert grammar.get_create_index_sql(
        ops.AddIndex('users', ['first_name'])
    ) == "CREATE INDEX first_name_idx ON users (first_name)"

    assert grammar.get_create_index_sql(
        ops.AddIndex('users', ['first_name'], 'myname_idx')
    ) == "CREATE INDEX myname_idx ON users (first_name)"

    assert grammar.get_create_index_sql(
        ops.AddIndex(
            table_name='users',
            columns=['first_name', 'last_name'],
            name='myname_idx',
            sort={'first_name': 'desc'},
        )
    ) == "CREATE INDEX myname_idx ON users (first_name DESC, last_name)"

    assert grammar.get_create_index_sql(
        ops.AddIndex(
            table_name='users',
            columns=['first_name'],
            name='myname_idx',
            sort={'first_name': 'desc nulls last'},
        )
    ) == "CREATE INDEX myname_idx ON users (first_name DESC NULLS LAST)"

    assert grammar.get_create_index_sql(
        ops.AddIndex(
            table_name='users',
            columns=['first_name'],
            name='myname_idx',
            sort={'first_name': 'desc nulls last'},
            operator_class='jsonb_path_ops'
        )
    ) == "CREATE INDEX myname_idx ON users (first_name jsonb_path_ops DESC NULLS LAST)"

    assert grammar.get_create_index_sql(
        ops.AddIndex(
            table_name='users',
            columns=['first_name'],
            name='myname_idx',
            sort={'first_name': 'desc nulls last'},
            operator_class='jsonb_path_ops',
            concurrently=True,
            using='btree',
        )
    ) == (
               "CREATE INDEX CONCURRENTLY myname_idx ON users USING btree "
               "(first_name jsonb_path_ops DESC NULLS LAST)"
           )

    assert grammar.get_create_index_sql(
        ops.AddIndex(
            table_name='users',
            columns=['first_name'],
            name='myname_idx',
            sort={'first_name': 'desc nulls last'},
            operator_class='jsonb_path_ops',
            concurrently=True,
            using='btree',
            unique=True
        )
    ) == (
               "CREATE UNIQUE INDEX CONCURRENTLY myname_idx ON users USING btree "
               "(first_name jsonb_path_ops DESC NULLS LAST)"
           )

    assert grammar.get_create_index_sql(
        ops.AddIndex(
            table_name='users',
            columns=['first_name'],
            name='myname_idx',
            sort={'first_name': 'desc nulls last'},
            operator_class='jsonb_path_ops',
            concurrently=True,
            using='btree',
            unique=True,
            if_not_exists=True,
        )
    ) == (
               "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS myname_idx ON users USING btree "
               "(first_name jsonb_path_ops DESC NULLS LAST)"
           )

    assert grammar.get_create_index_sql(
        ops.AddIndex(
            table_name='users',
            columns=['first_name'],
            name='myname_idx',
            sort={'first_name': 'desc nulls last'},
            operator_class='jsonb_path_ops',
            concurrently=True,
            using='btree',
            unique=True,
            if_not_exists=True,
            where='first_name is not null'
        )
    ) == (
               "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS myname_idx ON users USING btree "
               "(first_name jsonb_path_ops DESC NULLS LAST) WHERE first_name is not null"
           )


# endregion

# region DROP INDEX
def test_get_drop_index_sql(grammar):
    assert grammar.get_drop_index_sql(
        ops.DropIndex('myidx')
    ) == "DROP INDEX myidx"

    assert grammar.get_drop_index_sql(
        ops.DropIndex('myidx', if_exists=True)
    ) == "DROP INDEX IF EXISTS myidx"

    assert grammar.get_drop_index_sql(
        ops.DropIndex('myidx', if_exists=True, concurrently=True)
    ) == "DROP INDEX CONCURRENTLY IF EXISTS myidx"

    assert grammar.get_drop_index_sql(
        ops.DropIndex('myidx', if_exists=True, concurrently=True, cascade=True)
    ) == "DROP INDEX CONCURRENTLY IF EXISTS myidx CASCADE"


# endregion

# region CREATE SEQUENCE
def test_create_sequence_sql(grammar):
    assert grammar.get_create_sequence_sql(
        ops.CreateSequence(name='id_seq')
    ) == 'CREATE SEQUENCE id_seq'

    assert grammar.get_create_sequence_sql(
        ops.CreateSequence(name='id_seq', temporary=True)
    ) == 'CREATE TEMPORARY SEQUENCE id_seq'

    assert grammar.get_create_sequence_sql(
        ops.CreateSequence(name='id_seq', temporary=True, if_not_exists=True)
    ) == 'CREATE TEMPORARY SEQUENCE IF NOT EXISTS id_seq'

    assert grammar.get_create_sequence_sql(
        ops.CreateSequence(
            name='id_seq', temporary=True, if_not_exists=True, increment_by=5,
        )
    ) == 'CREATE TEMPORARY SEQUENCE IF NOT EXISTS id_seq INCREMENT BY 5'

    assert grammar.get_create_sequence_sql(
        ops.CreateSequence(
            name='id_seq', temporary=True, if_not_exists=True, increment_by=5,
            min_value=5
        )
    ) == (
               'CREATE TEMPORARY SEQUENCE IF NOT EXISTS id_seq '
               'INCREMENT BY 5 MINVALUE 5'
           )

    assert grammar.get_create_sequence_sql(
        ops.CreateSequence(
            name='id_seq', temporary=True, if_not_exists=True, increment_by=5,
            min_value=5, max_value=100
        )
    ) == (
               'CREATE TEMPORARY SEQUENCE IF NOT EXISTS id_seq '
               'INCREMENT BY 5 MINVALUE 5 MAXVALUE 100'
           )

    assert grammar.get_create_sequence_sql(
        ops.CreateSequence(
            name='id_seq', temporary=True, if_not_exists=True, increment_by=5,
            min_value=5, max_value=100, start_with=10
        )
    ) == (
               'CREATE TEMPORARY SEQUENCE IF NOT EXISTS id_seq '
               'INCREMENT BY 5 MINVALUE 5 MAXVALUE 100 START WITH 10'
           )

    assert grammar.get_create_sequence_sql(
        ops.CreateSequence(
            name='id_seq', temporary=True, if_not_exists=True, increment_by=5,
            min_value=5, max_value=100, start_with=10, cycle=True
        )
    ) == (
               'CREATE TEMPORARY SEQUENCE IF NOT EXISTS id_seq '
               'INCREMENT BY 5 MINVALUE 5 MAXVALUE 100 START WITH 10 CYCLE'
           )


# endregion

# region ALTER SEQUENCE
def test_alter_sequence_sql(grammar):
    assert grammar.get_alter_sequence_sql(
        ops.AlterSequence('id_seq')
    ) == 'ALTER SEQUENCE id_seq'

    assert grammar.get_alter_sequence_sql(
        ops.AlterSequence('id_seq', increment_by=5)
    ) == 'ALTER SEQUENCE id_seq INCREMENT BY 5'

    assert grammar.get_alter_sequence_sql(
        ops.AlterSequence('id_seq', increment_by=5, min_value=1)
    ) == 'ALTER SEQUENCE id_seq INCREMENT BY 5 MINVALUE 1'

    assert grammar.get_alter_sequence_sql(
        ops.AlterSequence('id_seq', increment_by=5, min_value=1, max_value=100)
    ) == 'ALTER SEQUENCE id_seq INCREMENT BY 5 MINVALUE 1 MAXVALUE 100'

    assert grammar.get_alter_sequence_sql(
        ops.AlterSequence(
            'id_seq', increment_by=5, min_value=1, max_value=100, start_with=0
        )
    ) == (
               'ALTER SEQUENCE id_seq INCREMENT BY 5 MINVALUE 1 MAXVALUE 100 '
               'START WITH 0'
           )

    assert grammar.get_alter_sequence_sql(
        ops.AlterSequence(
            'id_seq', increment_by=5, min_value=1, max_value=100, start_with=0,
            restart_with=100
        )
    ) == (
               'ALTER SEQUENCE id_seq INCREMENT BY 5 MINVALUE 1 MAXVALUE 100 '
               'START WITH 0 RESTART WITH 100'
           )

    assert grammar.get_alter_sequence_sql(
        ops.AlterSequence(
            'id_seq', increment_by=5, min_value=1, max_value=100, start_with=0,
            restart_with=100, cycle=True
        )
    ) == (
               'ALTER SEQUENCE id_seq INCREMENT BY 5 MINVALUE 1 MAXVALUE 100 '
               'START WITH 0 RESTART WITH 100 CYCLE'
           )


# endregion

# region RENAME SEQUENCE
def test_rename_sequence_sql(grammar):
    assert grammar.get_rename_sequence_sql(
        ops.RenameSequence('id_seq', 'new_id_seq')
    ) == 'ALTER SEQUENCE id_seq RENAME TO new_id_seq'


# endregion

# region CREATE VIEW
def test_create_view_sql(grammar):
    assert grammar.get_create_view_sql(
        ops.CreateView(name='myview', sql='SELECT 1 AS name')
    ) == 'CREATE VIEW myview AS SELECT 1 AS name'

    assert grammar.get_create_view_sql(
        ops.CreateView(name='myview', sql='SELECT 1 AS name', replace=True)
    ) == 'CREATE OR REPLACE VIEW myview AS SELECT 1 AS name'

    assert grammar.get_create_view_sql(
        ops.CreateView(name='myview', sql='SELECT 1 AS name', replace=True,
                       temporary=True)
    ) == 'CREATE OR REPLACE TEMPORARY VIEW myview AS SELECT 1 AS name'

    assert grammar.get_create_view_sql(
        ops.CreateView(name='myview', sql='SELECT 1 AS name', replace=True,
                       columns=['col_name'])
    ) == 'CREATE OR REPLACE VIEW myview (col_name) AS SELECT 1 AS name'

    sql = pypika.Table('users').select('id', 'email')
    assert grammar.get_create_view_sql(
        ops.CreateView(name='myview', sql=sql)
    ) == 'CREATE VIEW myview AS SELECT "id","email" FROM "users"'


# endregion

# region CREATE VIEW
def test_rename_view_sql(grammar):
    assert grammar.get_rename_view_sql(
        ops.RenameView(name='myview', new_name='new_myview')
    ) == 'ALTER VIEW myview RENAME TO new_myview'

    assert grammar.get_rename_view_sql(
        ops.RenameView(name='myview', new_name='new_myview', if_exists=True)
    ) == 'ALTER VIEW IF EXISTS myview RENAME TO new_myview'


# endregion

# region CREATE VIEW
def test_drop_view_sql(grammar):
    assert grammar.get_drop_view_sql(
        ops.DropView(name='myview')
    ) == 'DROP VIEW myview'

    assert grammar.get_drop_view_sql(
        ops.DropView(name='myview', if_exists=True)
    ) == 'DROP VIEW IF EXISTS myview'

    assert grammar.get_drop_view_sql(
        ops.DropView(name='myview', if_exists=True, cascade=True)
    ) == 'DROP VIEW IF EXISTS myview CASCADE'
# endregion
