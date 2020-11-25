class AerieException(Exception):
    pass


class DriverNotRegistered(AerieException):
    pass


class IntegrityError(AerieException):
    pass


class UniqueViolationError(IntegrityError):
    pass
