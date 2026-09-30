import dataclasses
import typing
from collections.abc import AsyncIterator, Sequence

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncResult, AsyncScalarResult, AsyncSession


class ResultDecoder[T](typing.Protocol):
    async def all(self, session: AsyncSession, statement: sa.Executable) -> list[T]: ...

    async def first(self, session: AsyncSession, statement: sa.Executable) -> T | None: ...

    async def one(self, session: AsyncSession, statement: sa.Executable) -> T: ...

    async def one_or_none(self, session: AsyncSession, statement: sa.Executable) -> T | None: ...

    def partitions(self, session: AsyncSession, statement: sa.Executable, size: int) -> AsyncIterator[Sequence[T]]: ...


@dataclasses.dataclass(frozen=True, slots=True)
class ScalarDecoder[T]:
    """Decodes a single-column result, which is also how ORM entities come back."""

    unique: bool = False
    """Deduplicate rows. Only ever set by the library, for joined collection loads."""

    async def _scalars(self, session: AsyncSession, statement: sa.Executable) -> sa.ScalarResult[typing.Any]:
        result: sa.ScalarResult[typing.Any] = await session.scalars(statement)
        return result.unique() if self.unique else result

    async def all(self, session: AsyncSession, statement: sa.Executable) -> list[T]:
        # ScalarResult.all() already returns a list; it is typed Sequence for variance.
        return typing.cast(list[T], (await self._scalars(session, statement)).all())

    async def first(self, session: AsyncSession, statement: sa.Executable) -> T | None:
        return typing.cast(T | None, (await self._scalars(session, statement)).first())

    async def one(self, session: AsyncSession, statement: sa.Executable) -> T:
        return typing.cast(T, (await self._scalars(session, statement)).one())

    async def one_or_none(self, session: AsyncSession, statement: sa.Executable) -> T | None:
        return typing.cast(T | None, (await self._scalars(session, statement)).one_or_none())

    async def partitions(
        self, session: AsyncSession, statement: sa.Executable, size: int
    ) -> AsyncIterator[Sequence[T]]:
        if self.unique:
            raise ValueError(
                "Streaming cannot deduplicate: a joined collection load needs the whole result "
                "in memory to unique it. Use selectinload() to stream, or all() to buffer."
            )
        statement = statement.execution_options(yield_per=size, stream_results=True)
        result: AsyncScalarResult[typing.Any] = await session.stream_scalars(statement)
        try:
            async for partition in result.partitions(size):
                yield typing.cast(Sequence[T], partition)
        finally:
            # `stream_results` opens a server-side portal; abandoning the generator
            # mid-iteration would otherwise leave it open on the connection.
            await result.close()


@dataclasses.dataclass(frozen=True, slots=True)
class RowDecoder[S]:
    """Decodes multi-column rows by handing each one to `assemble`.

    Tuples and read-models differ only in that callable, so they share this: the
    server-side portal teardown in `partitions` exists once rather than per shape.
    """

    assemble: typing.Callable[[typing.Sequence[typing.Any]], S]

    async def all(self, session: AsyncSession, statement: sa.Executable) -> list[S]:
        result: sa.Result[typing.Any] = await session.execute(statement)
        return [self.assemble(row) for row in result]

    async def first(self, session: AsyncSession, statement: sa.Executable) -> S | None:
        result: sa.Result[typing.Any] = await session.execute(statement)
        row = result.first()
        return None if row is None else self.assemble(row)

    async def one(self, session: AsyncSession, statement: sa.Executable) -> S:
        result: sa.Result[typing.Any] = await session.execute(statement)
        return self.assemble(result.one())

    async def one_or_none(self, session: AsyncSession, statement: sa.Executable) -> S | None:
        result: sa.Result[typing.Any] = await session.execute(statement)
        row = result.one_or_none()
        return None if row is None else self.assemble(row)

    async def partitions(
        self, session: AsyncSession, statement: sa.Executable, size: int
    ) -> AsyncIterator[Sequence[S]]:
        statement = statement.execution_options(yield_per=size, stream_results=True)
        result: AsyncResult[typing.Any] = await session.stream(statement)
        try:
            async for partition in result.partitions(size):
                yield [self.assemble(row) for row in partition]
        finally:
            # `stream_results` opens a server-side portal; abandoning the generator
            # mid-iteration would otherwise leave it open on the connection.
            await result.close()


def decoder_for(columns: tuple[typing.Any, ...]) -> typing.Any:
    """Pick the decoder a projection needs. One column decodes to scalars, more to tuples."""
    if len(columns) == 1:
        # Entities decode exactly like scalars -- one value per row.
        return ScalarDecoder[typing.Any]()
    return RowDecoder[typing.Any](assemble=tuple)
