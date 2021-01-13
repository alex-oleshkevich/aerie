from __future__ import annotations

import typing as t
from dataclasses import dataclass

import pypika as pk

from aerie.exceptions import AerieException
from aerie.protocols import Empty, StrLike
from aerie.schema import types
from aerie.schema.structure import Column

if t.TYPE_CHECKING:
    from aerie.drivers.base import BaseDriver

undefined = Empty()


class Operation:
    def get_sql(self, driver: "BaseDriver") -> str:
        pass


class RunSQL(Operation):
    def __init__(self, sql: str):
        self.sql = sql

    def get_sql(self, driver: "BaseDriver") -> str:
        return self.sql


@dataclass
class CreateTable(Operation):
    table_name: str
    columns: t.List[Column]
    temporary: bool = False
    if_not_exist: bool = False
    options_sql: str = None
    primary_key: t.Iterable[str] = None

    def __post_init__(self):
        if self.primary_key is None:
            self.primary_key = []

    def get_sql(self, driver: "BaseDriver") -> str:
        return driver.get_grammar().get_create_table_sql(self)


@dataclass
class RenameTable(Operation):
    table_name: str
    new_table_name: str

    def get_sql(self, driver: "BaseDriver") -> str:
        return driver.get_grammar().get_rename_table_sql(self)


@dataclass
class DropTable(Operation):
    table_name: str
    if_exists: bool = False

    def get_sql(self, driver: "BaseDriver") -> str:
        return driver.get_grammar().get_drop_table_sql(self)


@dataclass
class AddColumn(Operation):
    table_name: str
    column_name: str
    column_type: t.Union[str, types.BaseType]
    default: t.Any = undefined
    null: bool = None
    if_not_exists: bool = False

    def get_sql(self, driver: "BaseDriver") -> str:
        return driver.get_grammar().get_add_column_sql(self)


@dataclass
class RenameColumn(Operation):
    table_name: str
    column_name: str
    new_column_name: str
    if_exists: bool = False

    def get_sql(self, driver: "BaseDriver") -> str:
        return driver.get_grammar().get_rename_column_sql(self)


@dataclass
class DropColumn(Operation):
    table_name: str
    column_name: str
    if_exists: bool = False
    cascade: bool = False

    def get_sql(self, driver: "BaseDriver") -> str:
        return driver.get_grammar().get_drop_column_sql(self)


@dataclass
class AddIndex(Operation):
    table_name: str
    columns: t.Union[str, t.Iterable[str]]
    name: str = None
    sort: t.Dict[str, str] = None
    using: str = None
    where: t.Union[str, pk.terms.Term] = None
    operator_class: str = None
    unique: bool = False
    if_not_exists: bool = False
    concurrently: bool = False

    def __post_init__(self):
        self._build_columns()
        self._build_name()
        self._build_sorting()

    def get_sql(self, driver: "BaseDriver") -> str:
        return driver.get_grammar().get_create_index_sql(self)

    def _build_columns(self):
        if isinstance(self.columns, str):
            self.columns = [self.columns]

    def _build_name(self):
        if not self.name:
            self.name = "_".join([c for c in self.columns])
            if self.unique:
                self.name += "_unique"
            self.name += "_idx"

    def _build_sorting(self):
        if not self.sort:
            self.sort = {}
        if isinstance(self.sort, str):
            self.sort = {c: self.sort for c in self.columns}


@dataclass
class DropIndex(Operation):
    index_name: str
    if_exists: bool = False
    cascade: bool = False
    concurrently: bool = False

    def get_sql(self, driver: "BaseDriver") -> str:
        if self.concurrently and self.cascade and driver.dialect == "postgresql":
            raise AerieException(
                'Setting "cascade" and "concurrently" at same time '
                "is not supported by PostgreSQL."
            )
        return driver.get_grammar().get_drop_index_sql(self)


@dataclass
class CreateSequence(Operation):
    name: str
    temporary: bool = False
    if_not_exists: bool = False
    increment_by: int = None
    min_value: int = None
    max_value: int = None
    start_with: int = None
    cycle: bool = False

    def get_sql(self, driver: "BaseDriver") -> str:
        return driver.get_grammar().get_create_sequence_sql(self)


@dataclass
class AlterSequence(Operation):
    name: str
    increment_by: int = None
    min_value: int = None
    max_value: int = None
    start_with: int = None
    restart_with: int = None
    cycle: bool = None

    def get_sql(self, driver: "BaseDriver") -> str:
        return driver.get_grammar().get_alter_sequence_sql(self)


@dataclass
class RenameSequence(Operation):
    name: str
    new_name: str

    def get_sql(self, driver: "BaseDriver") -> str:
        return driver.get_grammar().get_rename_sequence_sql(self)


@dataclass
class DropSequence(Operation):
    name: str
    if_exists: bool = False
    temporary: bool = False
    cascade: bool = False

    def get_sql(self, driver: "BaseDriver") -> str:
        return driver.get_grammar().get_drop_sequence_sql(self)


@dataclass
class CreateView(Operation):
    name: str
    sql: StrLike
    replace: bool = False
    temporary: bool = False
    columns: t.Iterable[str] = None

    def get_sql(self, driver: "BaseDriver") -> str:
        return driver.get_grammar().get_create_view_sql(self)


@dataclass
class DropView(Operation):
    name: str
    if_exists: bool = False
    cascade: bool = False

    def get_sql(self, driver: "BaseDriver") -> str:
        return driver.get_grammar().get_drop_view_sql(self)


@dataclass
class RenameView(Operation):
    name: str
    new_name: str
    if_exists: bool = False

    def get_sql(self, driver: "BaseDriver") -> str:
        return driver.get_grammar().get_rename_view_sql(self)


@dataclass
class CreateForeignKey(Operation):
    local_table: str
    local_columns: t.Iterable[str]
    foreign_table: str
    foreign_columns: t.Iterable[str]
    on_delete: str = None
    on_update: str = None
    deferrable: bool = True
    initially_immediate: bool = True

    def get_sql(self, driver: "BaseDriver") -> str:
        return driver.get_grammar().get_create_foreign_key_sql(self)


@dataclass
class DropForeignKey(Operation):
    def get_sql(self, driver: "BaseDriver") -> str:
        return driver.get_grammar().drop_create_foreign_key_sql(self)
