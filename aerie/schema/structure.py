from __future__ import annotations

import typing as t
from dataclasses import dataclass

from aerie.schema.types import BaseType


class Sort:
    ASC = "asc"
    DESC = "desc"


@dataclass
class Schema:
    name: str
    tables: t.List[Table]

    def __iter__(self):
        return iter(self.tables)


@dataclass
class Table:
    schema: str
    name: str
    columns: t.List[Column]

    def __iter__(self):
        return iter(self.tables)


@dataclass
class Column:
    table: Table
    name: str
    type: BaseType
    default: t.Any
    null: bool


@dataclass
class IndexColumn:
    column: Column
    sort: str = Sort.ASC


@dataclass
class Index:
    name: str
    unique: bool
    columns: t.List[IndexColumn]
