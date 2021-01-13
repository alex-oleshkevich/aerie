from __future__ import annotations

import typing as t

import pypika as pk


class OnConflict:
    NOTHING = "nothing"
    REPLACE = "replace"
    RAISE = "raise"


class Raw(pk.terms.Term):
    def __init__(self, raw_sql: str, alias: t.Optional[str] = None) -> None:
        super().__init__(alias=alias)
        self.alias = alias
        self.raw_sql = raw_sql

    def get_sql(self, **kwargs: t.Any) -> str:
        return self.raw_sql


CurrentTimestamp = pk.terms.CustomFunction("CURRENT_TIMESTAMP")
Now = pk.terms.CustomFunction("NOW")


class OrderBy:
    ASC = "ASC"
    DESC = "ASC"

    NULLS_FIRST = "NULLS FIRST"
    NULLS_LAST = "NULLS LAST"

    def __init__(self, direction: str = None, nulls: str = None):
        self._direction = direction
        self._nulls = nulls

    def direction(self, direction: str) -> OrderBy:
        assert direction in [OrderBy.ASC, OrderBy.DESC]
        self._direction = direction
        return self

    def nulls(self, nulls: str) -> OrderBy:
        assert nulls in [OrderBy.NULLS_LAST, OrderBy.NULLS_FIRST]
        self._nulls = nulls
        return self

    def get_sql(self, **kwargs: t.Any) -> str:
        return " ".join(filter(lambda x: x, [self._direction, self._nulls])).upper()
