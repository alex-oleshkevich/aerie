from __future__ import annotations

import typing as t


class StrLike(t.Protocol):
    def __str__(self) -> str:
        ...


Queryable = t.Union[str, StrLike]


class Row(t.Mapping):
    pass
