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
