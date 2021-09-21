from sqlalchemy import exc


class TooManyResultsError(exc.MultipleResultsFound):
    """Raised when .one() query matches more that one result."""


class NoResultsError(exc.NoResultFound):
    """Raised when .one() matches no rows."""
