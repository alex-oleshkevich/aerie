from sqlalchemy import exc


class AerieError(Exception):  # pragma: no cover
    """Base class for aerie errors."""


class TooManyResultsError(AerieError, exc.MultipleResultsFound):  # pragma: no cover
    """Raised when .one() query matches more that one result."""


class NoResultsError(AerieError, exc.NoResultFound):  # pragma: no cover
    """Raised when .one() matches no rows."""


class NoActiveSessionError(AerieError):  # pragma: no cover
    """Raised when not global session exists."""
