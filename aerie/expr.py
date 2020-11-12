from __future__ import annotations

import typing as t


class OnConflict:
    DO_NOTHING = 'do_nothing'
    DO_UPDATE = 'do_update'

    def __init__(self):
        self.action = None
        self.column = None
        self.constraint = None
        self.update_where_clause = None
        self.update_values = None

    @property
    def target_fields(self) -> t.List[str]:
        return [x for x in [self.column, self.constraint] if x is not None]

    def on_column(self, column: str) -> OnConflict:
        self.column = column
        return self

    def on_constraint(self, constraint: str) -> OnConflict:
        self.constraint = constraint
        return self

    def do_nothing(self) -> OnConflict:
        self.action = self.DO_NOTHING
        return self

    def do_update(self, *, where=None, values: t.Dict) -> OnConflict:
        self.action = self.DO_UPDATE
        self.update_where_clause = where
        self.update_values = values
        return self


OnConflictHandler = t.Callable[[OnConflict], OnConflict]


class OrderBy:
    NULLS_LAST = 'last'
    NULLS_FIRST = 'first'
    ASC = 'asc'
    DESC = 'desc'

    def __init__(
            self, column: str = None, ordering: str = ASC, nulls: str = None,
    ):
        self._nulls = nulls
        self._column = column
        self._ordering = ordering

    def by(self, column: str):
        self._column = column
        return self

    def nulls_last(self) -> OrderBy:
        self._nulls = self.NULLS_LAST
        return self

    def nulls_first(self) -> OrderBy:
        self._nulls = self.NULLS_FIRST
        return self

    def asc(self) -> OrderBy:
        self._ordering = self.ASC
        return self

    def desc(self) -> OrderBy:
        self._ordering = self.DESC
        return self


OrderByArguments = t.Union[str, OrderBy, t.Callable[[OrderBy], OrderBy]]
