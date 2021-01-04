import typing as t

from dataclasses import dataclass


class BaseType:
    ...


@dataclass(frozen=True)
class Integer(BaseType):
    autoincrement: bool = False


@dataclass(frozen=True)
class BigInteger(Integer):
    ...


@dataclass(frozen=True)
class SmallInteger(Integer):
    ...


@dataclass(frozen=True)
class String(BaseType):
    length: int = 255


@dataclass(frozen=True)
class Text(BaseType):
    ...


@dataclass(frozen=True)
class Boolean(BaseType):
    ...


@dataclass(frozen=True)
class DateTime(BaseType):
    timezone: bool = False


@dataclass(frozen=True)
class Date(BaseType):
    ...


@dataclass(frozen=True)
class Time(BaseType):
    ...


@dataclass(frozen=True)
class Timestamp(BaseType):
    ...


@dataclass(frozen=True)
class Interval(BaseType):
    ...


@dataclass(frozen=True)
class Float(BaseType):
    precision: t.Optional[int] = None
    scale: t.Optional[int] = None


@dataclass(frozen=True)
class Decimal(Float):
    ...


@dataclass(frozen=True)
class Numeric(Float):
    ...


@dataclass(frozen=True)
class Binary(BaseType):
    ...


@dataclass(frozen=True)
class JSON(BaseType):
    ...


@dataclass(frozen=True)
class UUID(BaseType):
    ...


@dataclass(frozen=True)
class Enum(BaseType):
    values: t.List[str]


@dataclass(frozen=True)
class IPAddress(BaseType):
    ...


@dataclass(frozen=True)
class MACAddress(BaseType):
    ...


@dataclass(frozen=True)
class Array(BaseType):
    child_type: BaseType
