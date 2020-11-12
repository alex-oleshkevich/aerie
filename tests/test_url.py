import pytest

from aerie.url import URL


@pytest.fixture
def url():
    return URL(
        'psql+aiopg://username:password@localhost:5432/dbname?pool_min=5&timeout=10'
    )


def test_scheme(url):
    assert url.scheme == 'psql+aiopg'


def test_driver(url):
    assert url.driver == 'psql'


def test_library(url):
    assert url.library == 'aiopg'


def test_username(url):
    assert url.username == 'username'


def test_password(url):
    assert url.password == 'password'


def test_hostname(url):
    assert url.hostname == 'localhost'


def test_port(url):
    assert url.port == 5432


def test_dbname(url):
    assert url.db_name == 'dbname'


def test_options(url):
    assert url.options == {'pool_min': '5', 'timeout': '10'}


def test_to_string(url):
    assert str(
        url
    ) == 'psql+aiopg://username:******@localhost:5432/dbname?pool_min=5&timeout=10'
