from __future__ import annotations

import typing as t

from sqlalchemy.ext.asyncio import AsyncSession

from aerie.models import Model
from aerie.queries import SelectQuery

M = t.TypeVar('M', bound=Model)


class DbSession(AsyncSession):

    def query(self, model: t.Type[M]) -> SelectQuery[M]:
        return SelectQuery(model, self)

    def add(self, *objects: M) -> None:
        super().add(objects)
