from __future__ import annotations

import contextvars as cv
import typing as t
from sqlalchemy.ext.asyncio import AsyncSession

from aerie.exceptions import NoActiveSessionError
from aerie.models import Model
from aerie.queries import SelectQuery

M = t.TypeVar('M', bound=Model)


class DbSession(AsyncSession):
    current_session_stack: cv.ContextVar[list[DbSession]] = cv.ContextVar('current_session_stack', default=[])
    """A list of DbSession instances associated with current task."""

    def query(self, model: t.Type[M]) -> SelectQuery[M]:
        return SelectQuery(model, self)

    def add(self, *objects: M) -> None:
        """Add objects to session."""
        if len(objects) > 1:
            self.add_all(objects)
        else:
            super().add(objects[0])

    def add_all(self, objects: t.Iterable[M]) -> None:
        super().add_all(objects)

    @classmethod
    def get_current_session(cls) -> DbSession:
        """Return the last activated session associated with current asyncio task."""
        try:
            stack = cls.current_session_stack.get()
            return stack[len(stack) - 1]
        except IndexError:
            raise NoActiveSessionError()

    async def close(self) -> None:
        await super().close()
        DbSession.current_session_stack.get().remove(self)

    @classmethod
    async def close_all(cls) -> None:
        DbSession.current_session_stack.get().clear()
        await super().close_all()

    async def __aenter__(self) -> DbSession:
        DbSession.current_session_stack.get().append(self)
        return self
