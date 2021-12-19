import typing as t
from contextlib import contextmanager
from sqlalchemy.exc import MultipleResultsFound, NoResultFound

from aerie.exceptions import NoResultsError, TooManyResultsError


@contextmanager
def convert_exceptions() -> t.Generator[None, None, None]:
    try:
        yield
    except MultipleResultsFound as exc:
        raise TooManyResultsError from exc
    except NoResultFound as exc:
        raise NoResultsError from exc


ITEM = t.TypeVar('ITEM')


def chunked(items: t.Iterable[ITEM], size: int) -> t.Generator[t.List[ITEM], None, None]:
    result = []
    for value in items:
        result.append(value)
        if len(result) == size:
            yield result
            result = []

    if len(result):
        yield result


def colorize(sql: str) -> str:
    try:
        import pygments
        import pygments.lexers
        import pygments.formatters

        lexer = pygments.lexers.get_lexer_by_name("sql")
        formatter = pygments.formatters.get_formatter_by_name("console")
        sql = pygments.highlight(sql, lexer, formatter)
    except ImportError:
        pass
    return sql
