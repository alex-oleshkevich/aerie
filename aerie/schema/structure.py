from __future__ import annotations

import dataclasses
import typing as t
from dataclasses import dataclass

from aerie.schema.types import BaseType
from aerie.terms import OrderBy


class Sort:
    ASC = "asc"
    DESC = "desc"


@dataclass
class Schema:
    name: str
    tables: t.List[Table] = dataclasses.field(default_factory=list)

    def __iter__(self):
        return iter(self.tables)


@dataclass
class Sequence:
    name: str
    schema: str = None


@dataclass
class View:
    name: str
    sql: str


@dataclass
class Table:
    name: str
    schema: str = None
    columns: t.List[Column] = dataclasses.field(default_factory=list)

    def __iter__(self):
        return iter(self.columns)


T = t.TypeVar("T", bound=BaseType)


@dataclass
class Column(t.Generic[T]):
    name: str
    type: T
    default: t.Any = None
    null: bool = None


@dataclass
class IndexColumn:
    column: Column
    sort: OrderBy = None
    length: int = None
    opclass: str = None


@dataclass
class Index:
    name: str
    unique: bool
    columns: t.List[IndexColumn] = dataclasses.field(default_factory=list)
