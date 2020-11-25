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
