"""ORM-enabled UPDATE and DELETE.

Both are built from a query's WHERE clause, so `query(User).where(...).update()` targets
exactly the rows the equivalent SELECT would have returned. RETURNING reuses the same
result-shape machinery as `select()`: one column decodes to scalars, several to tuples,
the model itself to entities.
"""

import dataclasses
import typing

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase

from aerie.decoders import ResultDecoder, decoder_for


@dataclasses.dataclass(frozen=True, slots=True)
class MutationResult:
    """What a write changed."""

    rowcount: int


@dataclasses.dataclass(frozen=True, slots=True)
class ReturningQuery[T]:
    """The rows a write handed back. Terminals mirror a read query's."""

    statement: sa.Executable
    decoder: ResultDecoder[T]

    async def all(self, session: AsyncSession) -> list[T]:
        return await self.decoder.all(session, self.statement)

    async def first(self, session: AsyncSession) -> T | None:
        return await self.decoder.first(session, self.statement)

    async def one(self, session: AsyncSession) -> T:
        return await self.decoder.one(session, self.statement)

    async def one_or_none(self, session: AsyncSession) -> T | None:
        return await self.decoder.one_or_none(session, self.statement)


@dataclasses.dataclass(frozen=True, slots=True)
class _Mutation[M: DeclarativeBase]:
    model: type[M]
    whereclause: sa.ColumnElement[bool] = dataclasses.field(default_factory=sa.true)
    """Always a predicate: `EntityQuery` projects its primary key through the query."""

    def _restrict[S: sa.Update | sa.Delete](self, statement: S) -> S:
        return typing.cast(S, statement.where(self.whereclause))

    def statement(self) -> sa.Update | sa.Delete:
        raise NotImplementedError

    def returning(self, *columns: typing.Any) -> ReturningQuery[typing.Any]:
        return ReturningQuery(
            statement=self.statement().returning(*columns),
            decoder=decoder_for(columns),
        )

    async def execute(self, session: AsyncSession) -> MutationResult:
        result = await session.execute(self.statement())
        return MutationResult(rowcount=typing.cast(typing.Any, result).rowcount)


@dataclasses.dataclass(frozen=True, slots=True)
class UpdateQuery[M: DeclarativeBase](_Mutation[M]):
    assignments: tuple[tuple[sa.SQLColumnExpression[typing.Any], typing.Any], ...] = ()

    def set[V](self, field: sa.SQLColumnExpression[V], value: V | sa.SQLColumnExpression[V]) -> typing.Self:
        return dataclasses.replace(self, assignments=(*self.assignments, (field, value)))

    def inc[V](self, field: sa.SQLColumnExpression[V], amount: V) -> typing.Self:
        """Increment in the database, so concurrent writers do not clobber each other."""
        return self.set(field, typing.cast(typing.Any, field + amount))

    def dec[V](self, field: sa.SQLColumnExpression[V], amount: V) -> typing.Self:
        return self.set(field, typing.cast(typing.Any, field - amount))

    def statement(self) -> sa.Update:
        if not self.assignments:
            raise ValueError("update() needs at least one set()/inc()/dec() before it can run.")
        values = {typing.cast(typing.Any, field).key: value for field, value in self.assignments}
        return self._restrict(sa.update(self.model)).values(values)


@dataclasses.dataclass(frozen=True, slots=True)
class DeleteQuery[M: DeclarativeBase](_Mutation[M]):
    def statement(self) -> sa.Delete:
        return self._restrict(sa.delete(self.model))
