import pytest
from aerie import Database


@pytest.fixture()
def db() -> Database:
    return Database('psql://localhost')


def test_insert_query(db):
    query = db.insert(
        table='users',
        values={
            'id': 1,
            'email': 'root@localhost',
        },
    )
    assert query.get_sql() == (
        'INSERT INTO "users" ("id","email") '
        'VALUES (1,\'root@localhost\')'
    )


def test_insert_all_query(db):
    query = db.insert_many(
        table='users',
        values=[
            {'id': 1, 'email': 'one@localhost'},
            {'id': 2, 'email': 'two@localhost'},
        ],
    )
    assert query.get_sql() == (
        'INSERT INTO "users" ("id","email") '
        'VALUES ({\'id\': 1, \'email\': \'one@localhost\'},'
        '{\'id\': 2, \'email\': \'two@localhost\'})'
    )


def test_insert_query_returning(db):
    query = db.insert(
        table='users',
        values={
            'id': 1,
            'email': 'root@localhost',
        },
        returning=['id', 'email']
    )
    assert query.get_sql() == (
        'INSERT INTO "users" ("id","email") '
        'VALUES (1,\'root@localhost\') '
        'RETURNING "id","email"'
    )


def test_insert_query_on_conflict_do_nothing(db):
    query = db.insert(
        table='users',
        values={
            'id': 1,
            'email': 'root@localhost',
        },
        on_conflict=lambda b: b.on_column('id').do_nothing()
    )
    assert query.get_sql() == (
        'INSERT INTO "users" ("id","email") '
        'VALUES (1,\'root@localhost\') '
        'ON CONFLICT ("id") DO NOTHING'
    )


def test_insert_query_on_conflict_do_update(db):
    query = db.insert(
        table='users',
        values={
            'id': 1,
            'email': 'root@localhost',
        },
        on_conflict=lambda b: b.on_column('id').do_update(
            values={'email': 'user@localhost', 'id': 2}
        )
    )
    assert query.get_sql() == (
        'INSERT INTO "users" ("id","email") '
        'VALUES (1,\'root@localhost\') '
        'ON CONFLICT ("id") DO UPDATE SET "email"=\'user@localhost\',"id"=2'
    )
