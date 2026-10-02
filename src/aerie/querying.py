import dataclasses
import typing

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm.interfaces import ORMOption

from aerie.decoders import ResultDecoder, RowDecoder, ScalarDecoder, decoder_for
from aerie.paging import CursorPage, Page

if typing.TYPE_CHECKING:
    from aerie.mutation import DeleteQuery, UpdateQuery


type AnySelect = sa.Select[*tuple[typing.Any, ...]]
type AnyStatement = AnySelect | sa.Insert | sa.Update | sa.Delete


@dataclasses.dataclass(frozen=True, slots=True)
class Query[T]:
    _statement: sa.Executable
    decoder: ResultDecoder[T]
    _loader_options: tuple[ORMOption, ...] = dataclasses.field(default=(), kw_only=True)

    @property
    def statement(self) -> sa.Executable:
        """The underlying SQLAlchemy statement, for anything this API does not cover."""
        if not self._loader_options:
            return self._statement
        return typing.cast(AnySelect, self._statement).options(*self._loader_options)

    def _select(self) -> AnySelect:
        return typing.cast(AnySelect, self._statement)

    def transform(self, fn: typing.Callable[[AnySelect], AnySelect]) -> typing.Self:
        """Apply an arbitrary statement transform, preserving the declared result shape."""
        return self._replace(fn(self._select()))

    def pipe[**P, R](
        self,
        fn: typing.Callable[typing.Concatenate[typing.Self, P], R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        """Hand this query to `fn`."""
        return fn(self, *args, **kwargs)

    def where(self, *predicates: sa.ColumnElement[bool]) -> typing.Self:
        return self._replace(self._select().where(*predicates))

    def when(self, condition: bool, callback: typing.Callable[[], sa.ColumnElement[bool]], /) -> typing.Self:
        """Apply a lazily-built predicate when ``condition`` is true."""
        if not condition:
            return self
        return self.where(callback())

    def when_not(self, condition: bool, callback: typing.Callable[[], sa.ColumnElement[bool]], /) -> typing.Self:
        """Apply a lazily-built predicate when ``condition`` is false."""
        return self.when(not condition, callback)

    def join(self, target: typing.Any, on: typing.Any = None, *, full: bool = False) -> typing.Self:
        statement = self._select().join(target, on, isouter=False, full=full)
        return self._replace(statement)

    def left_join(self, target: typing.Any, on: typing.Any = None, *, full: bool = False) -> typing.Self:
        statement = self._select().join(target, on, isouter=True, full=full)
        return self._replace(statement)

    def order_by(self, *expressions: typing.Any) -> typing.Self:
        return self._replace(self._select().order_by(*expressions))

    def group_by(self, *expressions: typing.Any) -> typing.Self:
        statement = self._select().group_by(*expressions)
        return self._replace(statement)

    def having(self, *predicates: sa.ColumnElement[bool]) -> typing.Self:
        return self._replace(self._select().having(*predicates))

    def distinct(self, *expressions: typing.Any) -> typing.Self:
        return self._replace(self._select().distinct(*expressions))

    def limit(self, value: int | None) -> typing.Self:
        return self._replace(self._select().limit(value))

    def offset(self, value: int | None) -> typing.Self:
        return self._replace(self._select().offset(value))

    def lock(self, *, nowait: bool = False, skip_locked: bool = False, read: bool = False) -> typing.Self:
        statement = self._select().with_for_update(nowait=nowait, skip_locked=skip_locked, read=read)
        return self._replace(statement)

    def options(self, *loader_options: ORMOption) -> typing.Self:
        """Attach SQLAlchemy ORM options such as ``selectinload`` or ``load_only``."""
        return self._replace(self._statement, _loader_options=self._loader_options + loader_options)

    def unique(self) -> typing.Self:
        """Deduplicate ORM entities after a joined collection load."""
        if not isinstance(self.decoder, ScalarDecoder):
            raise TypeError("unique() is only available for scalar ORM results")
        return self._replace(self._statement, decoder=dataclasses.replace(self.decoder, unique=True))

    @typing.overload
    def select[A](self, a: sa.SQLColumnExpression[A], /) -> ScalarQuery[A]: ...

    @typing.overload
    def select[A, B](self, a: sa.SQLColumnExpression[A], b: sa.SQLColumnExpression[B], /) -> TupleQuery[A, B]: ...

    @typing.overload
    def select[A, B, C](
        self, a: sa.SQLColumnExpression[A], b: sa.SQLColumnExpression[B], c: sa.SQLColumnExpression[C], /
    ) -> TupleQuery[A, B, C]: ...

    @typing.overload
    def select[A, B, C, D](
        self,
        a: sa.SQLColumnExpression[A],
        b: sa.SQLColumnExpression[B],
        c: sa.SQLColumnExpression[C],
        d: sa.SQLColumnExpression[D],
        /,
    ) -> TupleQuery[A, B, C, D]: ...

    @typing.overload
    def select[A, B, C, D, E](
        self,
        a: sa.SQLColumnExpression[A],
        b: sa.SQLColumnExpression[B],
        c: sa.SQLColumnExpression[C],
        d: sa.SQLColumnExpression[D],
        e: sa.SQLColumnExpression[E],
        /,
    ) -> TupleQuery[A, B, C, D, E]: ...

    @typing.overload
    def select[A, B, C, D, E, F](
        self,
        a: sa.SQLColumnExpression[A],
        b: sa.SQLColumnExpression[B],
        c: sa.SQLColumnExpression[C],
        d: sa.SQLColumnExpression[D],
        e: sa.SQLColumnExpression[E],
        f: sa.SQLColumnExpression[F],
        /,
    ) -> TupleQuery[A, B, C, D, E, F]: ...

    @typing.overload
    def select[A, B, C, D, E, F, G](
        self,
        a: sa.SQLColumnExpression[A],
        b: sa.SQLColumnExpression[B],
        c: sa.SQLColumnExpression[C],
        d: sa.SQLColumnExpression[D],
        e: sa.SQLColumnExpression[E],
        f: sa.SQLColumnExpression[F],
        g: sa.SQLColumnExpression[G],
        /,
    ) -> TupleQuery[A, B, C, D, E, F, G]: ...

    @typing.overload
    def select[A, B, C, D, E, F, G, H](
        self,
        a: sa.SQLColumnExpression[A],
        b: sa.SQLColumnExpression[B],
        c: sa.SQLColumnExpression[C],
        d: sa.SQLColumnExpression[D],
        e: sa.SQLColumnExpression[E],
        f: sa.SQLColumnExpression[F],
        g: sa.SQLColumnExpression[G],
        h: sa.SQLColumnExpression[H],
        /,
    ) -> TupleQuery[A, B, C, D, E, F, G, H]: ...

    @typing.overload
    def select[A, B, C, D, E, F, G, H, I](
        self,
        a: sa.SQLColumnExpression[A],
        b: sa.SQLColumnExpression[B],
        c: sa.SQLColumnExpression[C],
        d: sa.SQLColumnExpression[D],
        e: sa.SQLColumnExpression[E],
        f: sa.SQLColumnExpression[F],
        g: sa.SQLColumnExpression[G],
        h: sa.SQLColumnExpression[H],
        i: sa.SQLColumnExpression[I],
        /,
    ) -> TupleQuery[A, B, C, D, E, F, G, H, I]: ...

    @typing.overload
    def select[A, B, C, D, E, F, G, H, I, J](
        self,
        a: sa.SQLColumnExpression[A],
        b: sa.SQLColumnExpression[B],
        c: sa.SQLColumnExpression[C],
        d: sa.SQLColumnExpression[D],
        e: sa.SQLColumnExpression[E],
        f: sa.SQLColumnExpression[F],
        g: sa.SQLColumnExpression[G],
        h: sa.SQLColumnExpression[H],
        i: sa.SQLColumnExpression[I],
        j: sa.SQLColumnExpression[J],
        /,
    ) -> TupleQuery[A, B, C, D, E, F, G, H, I, J]: ...

    @typing.overload
    def select[A, B, C, D, E, F, G, H, I, J, K](
        self,
        a: sa.SQLColumnExpression[A],
        b: sa.SQLColumnExpression[B],
        c: sa.SQLColumnExpression[C],
        d: sa.SQLColumnExpression[D],
        e: sa.SQLColumnExpression[E],
        f: sa.SQLColumnExpression[F],
        g: sa.SQLColumnExpression[G],
        h: sa.SQLColumnExpression[H],
        i: sa.SQLColumnExpression[I],
        j: sa.SQLColumnExpression[J],
        k: sa.SQLColumnExpression[K],
        /,
    ) -> TupleQuery[A, B, C, D, E, F, G, H, I, J, K]: ...

    @typing.overload
    def select[A, B, C, D, E, F, G, H, I, J, K, L](
        self,
        a: sa.SQLColumnExpression[A],
        b: sa.SQLColumnExpression[B],
        c: sa.SQLColumnExpression[C],
        d: sa.SQLColumnExpression[D],
        e: sa.SQLColumnExpression[E],
        f: sa.SQLColumnExpression[F],
        g: sa.SQLColumnExpression[G],
        h: sa.SQLColumnExpression[H],
        i: sa.SQLColumnExpression[I],
        j: sa.SQLColumnExpression[J],
        k: sa.SQLColumnExpression[K],
        l: sa.SQLColumnExpression[L],
        /,
    ) -> TupleQuery[A, B, C, D, E, F, G, H, I, J, K, L]: ...

    def select(self, *columns: sa.SQLColumnExpression[typing.Any]) -> typing.Any:
        """Narrow the result to the given columns.

        One column yields a `ScalarQuery[T]`, more than one a `TupleQuery[*Ts]`. The
        FROM and WHERE built so far are kept; only what is selected changes.
        """
        statement = self._select().with_only_columns(*columns, maintain_column_froms=True)
        # Loader options are dropped: there is no entity left to load them onto, and
        # SQLAlchemy rejects a relationship loader on an expression-only statement.
        if len(columns) == 1:
            return ScalarQuery(_statement=statement, decoder=decoder_for(columns))
        return TupleQuery(_statement=statement, decoder=decoder_for(columns))

    def shape[S](self, target: typing.Callable[..., S]) -> ShapeQuery[S]:
        """Decode each row selected by this query as ``target(*row)``."""
        return ShapeQuery(
            _statement=self._statement,
            _loader_options=self._loader_options,
            decoder=RowDecoder(assemble=lambda row: target(*row)),
        )

    async def all(self, session: AsyncSession) -> list[T]:
        return await self.decoder.all(session, self.statement)

    async def execute(self, session: AsyncSession) -> sa.Result[typing.Any]:
        """Execute the underlying statement without decoding its rows."""
        return await session.execute(self.statement)

    async def first(self, session: AsyncSession) -> T | None:
        # LIMIT 1 rather than fetching every row and discarding all but one.
        return await self.decoder.first(session, self.limit(1).statement)

    async def first_or_raise(self, session: AsyncSession, exc: Exception | None = None) -> T:
        row = await self.first(session)
        if row is None:
            raise exc or NoResultFound("No row was found for the given query.")
        return row

    async def one(self, session: AsyncSession) -> T:
        return await self.decoder.one(session, self.statement)

    async def one_or_none(self, session: AsyncSession) -> T | None:
        return await self.decoder.one_or_none(session, self.statement)

    async def one_or_raise(self, session: AsyncSession, exc: Exception | None = None) -> T:
        value = await self.one_or_none(session)
        if value is None:
            raise exc or NoResultFound("No row was found for the given query.")
        return value

    async def exists(self, session: AsyncSession) -> bool:
        statement = sa.select(sa.exists(self._select().order_by(None)))
        return bool(await session.scalar(statement))

    async def count(self, session: AsyncSession) -> int:
        """Rows this query would return -- bounds included.

        `query(User).limit(100).count(session)` is at most 100. A paginator wanting the unbounded
        total must use an explicitly unbounded query instead.
        """
        subquery = self._select().order_by(None).subquery()
        total = await session.scalar(sa.select(sa.func.count()).select_from(subquery))
        return total or 0

    async def sum[V](self, session: AsyncSession, expression: sa.SQLColumnExpression[V]) -> V | None:
        return await self._aggregate(session, sa.func.sum, expression)

    async def avg[V](self, session: AsyncSession, expression: sa.SQLColumnExpression[V]) -> V | None:
        return await self._aggregate(session, sa.func.avg, expression)

    async def min[V](self, session: AsyncSession, expression: sa.SQLColumnExpression[V]) -> V | None:
        return await self._aggregate(session, sa.func.min, expression)

    async def max[V](self, session: AsyncSession, expression: sa.SQLColumnExpression[V]) -> V | None:
        return await self._aggregate(session, sa.func.max, expression)

    async def batches(self, session: AsyncSession, *, size: int = 1000) -> typing.AsyncIterator[typing.Sequence[T]]:
        async for batch in self.decoder.partitions(session, self.statement, size):
            yield batch

    async def iter(self, session: AsyncSession, *, batch_size: int = 1000) -> typing.AsyncIterator[T]:
        async for batch in self.batches(session, size=batch_size):
            for row in batch:
                yield row

    async def paginate(self, session: AsyncSession, *, number: int, size: int) -> Page[T]:
        """One page, plus the total the query would return unbounded.

        The total comes from an explicitly unbounded query rather than `count()`, which
        respects the query's own LIMIT on purpose.
        """
        total = await self._replace(self._select().order_by(None).limit(None).offset(None)).count(session)
        items = await self.limit(size).offset(max(0, number - 1) * size).all(session)
        return Page(items=items, total=total, number=number, size=size)

    def sql(self, *, literal: bool = False) -> str:
        """Render this query as SQL, compiled for PostgreSQL.

        The default dialect silently drops dialect-specific clauses -- `SKIP LOCKED`
        and `FOR SHARE` both come out as a bare `FOR UPDATE` -- which makes it useless
        for debugging the SQL that will actually run.
        """
        compile_kwargs = {"literal_binds": literal}
        # `postgresql.dialect` is an untyped alias in the SQLAlchemy stubs.
        dialect = typing.cast(typing.Any, postgresql.dialect)()
        return str(typing.cast(typing.Any, self.statement).compile(dialect=dialect, compile_kwargs=compile_kwargs))

    def _replace(self, statement: sa.Executable, **kwargs: typing.Any) -> typing.Self:
        return dataclasses.replace(self, _statement=statement, **kwargs)

    async def _aggregate[V](
        self,
        session: AsyncSession,
        function: typing.Callable[[sa.SQLColumnExpression[V]], sa.ColumnElement[typing.Any]],
        expression: sa.SQLColumnExpression[V],
    ) -> V | None:
        """Aggregate over exactly the rows this query returns.

        The aggregand is *added* to the target list rather than replacing it, then
        aggregated outside the subquery. Replacing it would move DISTINCT from whole
        rows onto the single projected column and drop the ORDER BY that LIMIT depends
        on -- both silently wrong. Adding keeps every clause meaning what it meant.
        """
        statement = self._select()
        if statement._group_by_clauses:
            raise ValueError(
                "Aggregating a grouped query is ambiguous: the aggregand is not itself grouped, "
                "so the database rejects it. Aggregate before group_by(), or build the grouped "
                "statement yourself via `.statement`."
            )

        rows = statement.add_columns(expression.label("value")).subquery()
        statement = sa.select(function(rows.c.value))
        return typing.cast(V | None, await session.scalar(statement))


@dataclasses.dataclass(frozen=True, slots=True)
class ScalarQuery[T](Query[T]):
    """A single-column result."""

    async def set(self, session: AsyncSession) -> set[T]:
        return set(await self.all(session))


@dataclasses.dataclass(frozen=True, slots=True)
class TupleQuery[*Ts](Query[tuple[*Ts]]):
    """A multi-column result, decoded as plain tuples."""


@dataclasses.dataclass(frozen=True, slots=True)
class ShapeQuery[S](Query[S]):
    """Detached read-models rather than ORM entities."""


@dataclasses.dataclass(frozen=True, slots=True)
class EntityQuery[M: DeclarativeBase](Query[M]):
    """A result of ORM entities."""

    model: type[M]

    @classmethod
    def for_model(cls, model: type[M] | None = None) -> typing.Self:
        """Open a query over `model`, or over the model a subclass binds in `EntityQuery[Model]`."""
        model = model or cls._bound_model()
        # Nothing loads implicitly: an un-preloaded relationship must fail with an
        # error that names the attribute, not a MissingGreenlet from the plumbing.
        return cls(
            _statement=sa.select(model),
            _loader_options=(sa.orm.raiseload("*"),),
            decoder=ScalarDecoder[M](),
            model=model,
        )

    @classmethod
    def _bound_model(cls) -> type[M]:
        for klass in cls.__mro__:
            for base in getattr(klass, "__orig_bases__", ()):
                origin = typing.get_origin(base)
                if isinstance(origin, type) and issubclass(origin, EntityQuery):
                    model = typing.get_args(base)[0]
                    if isinstance(model, type):
                        return typing.cast(type[M], model)
        raise TypeError(f"{cls.__name__} binds no model: subclass EntityQuery[Model] or pass the model")

    @typing.overload
    async def map[K](self, session: AsyncSession, key: sa.SQLColumnExpression[K], /) -> dict[K, M]: ...

    @typing.overload
    async def map[K, V](
        self, session: AsyncSession, key: sa.SQLColumnExpression[K], value: sa.SQLColumnExpression[V], /
    ) -> dict[K, V]: ...

    async def map(
        self,
        session: AsyncSession,
        key: sa.SQLColumnExpression[typing.Any],
        value: sa.SQLColumnExpression[typing.Any] | None = None,
    ) -> dict[typing.Any, typing.Any]:
        """Materialise the result as a dict.

        With one column, entities keyed by it. With two, a plain column-to-column map,
        which selects only those two columns rather than loading whole entities.
        """
        if value is None:
            name = typing.cast(typing.Any, key).key
            return {getattr(row, name): row for row in await self.all(session)}
        return dict(await self.select(key, value).all(session))

    def update(self) -> UpdateQuery[M]:
        """Turn this query's WHERE clause into an UPDATE against the same rows."""
        from aerie.mutation import UpdateQuery

        return UpdateQuery(model=self.model, whereclause=self._targets_this_query())

    def delete(self) -> DeleteQuery[M]:
        """Turn this query's WHERE clause into a DELETE against the same rows."""
        from aerie.mutation import DeleteQuery

        return DeleteQuery(model=self.model, whereclause=self._targets_this_query())

    def _targets_this_query(self) -> sa.ColumnElement[bool]:
        """A predicate selecting exactly the rows this query returns.

        UPDATE and DELETE carry a WHERE clause and nothing else -- no join, no LIMIT,
        no GROUP BY. Rather than restricting which queries may be written, project the
        primary key through the query as it stands and match on that, which keeps every
        clause and works for composite keys.
        """
        statement = self._select()
        if self._is_plain(statement):
            return statement.whereclause if statement.whereclause is not None else sa.true()

        primary_key = sa.inspect(self.model).primary_key
        rows = statement.with_only_columns(*primary_key, maintain_column_froms=True)
        return sa.tuple_(*primary_key).in_(rows)

    def _is_plain(self, statement: sa.Select[typing.Any]) -> bool:
        """Whether the query reads one table with nothing but a WHERE clause.

        Then its predicate can go on the UPDATE or DELETE directly. That is not only
        shorter: under READ COMMITTED a writer that waited on a row lock re-evaluates a
        direct predicate against the committed row, but not the snapshot of an
        `IN (SELECT ...)`, so only the direct form is an atomic compare-and-set.
        """
        return (
            statement.get_final_froms() == [sa.inspect(self.model).local_table]
            and statement._limit_clause is None
            and statement._offset_clause is None
            and not statement._group_by_clauses
            and not statement._having_criteria
            and not statement._distinct
        )

    def pk(self, *values: typing.Any) -> typing.Self:
        """Filter by primary key on top of whatever this query already restricts.

        Unlike `Session.get`, this respects the current query -- which is what makes
        `repo(User).where(User.tenant_id == t).pk(id)` a tenant-safe lookup.
        """
        columns = sa.inspect(self.model).primary_key
        if len(values) != len(columns):
            raise ValueError(f"{self.model.__name__} has {len(columns)} primary key column(s), got {len(values)}.")
        return self.where(*[column == value for column, value in zip(columns, values, strict=True)])

    async def cursor_page(
        self,
        session: AsyncSession,
        *,
        size: int,
        by: typing.Sequence[sa.SQLColumnExpression[typing.Any]],
        after: tuple[typing.Any, ...] | None = None,
        descending: bool = False,
    ) -> CursorPage[M]:
        """A keyset page ordered by `by`, resuming after the given cursor.

        Comparison is done as a row value, so a multi-column cursor stays correct at
        ties. The columns must be jointly unique, or rows are skipped or repeated.
        """
        columns = tuple(by)
        query = self._replace(self._select().order_by(None))
        query = query.order_by(*[column.desc() if descending else column.asc() for column in columns])
        if after is not None:
            row = sa.tuple_(*columns)
            marker = sa.tuple_(*[sa.literal(value) for value in after])
            query = query.where(row < marker if descending else row > marker)

        items = await query.limit(size).all(session)
        cursor = None
        if items and len(items) == size:
            cursor = tuple(getattr(items[-1], typing.cast(typing.Any, column).key) for column in columns)
        return CursorPage(items=items, next_cursor=cursor)

    async def batches_by(
        self, session: AsyncSession, column: sa.SQLColumnExpression[typing.Any], *, size: int = 1000
    ) -> typing.AsyncIterator[typing.Sequence[M]]:
        """Walk the result in keyset batches, ordered by `column` ascending.

        Unlike `batches()`, this holds no server-side cursor between batches, so it is
        the safe choice for long-running jobs against a small connection pool. The
        column must be unique and stable, or rows will be skipped or repeated.
        """
        cursor: tuple[typing.Any, ...] | None = None
        while True:
            page = await self.cursor_page(session, size=size, by=[column], after=cursor)
            if not page.items:
                return
            yield page.items
            if page.next_cursor is None:
                return
            cursor = page.next_cursor

    async def iter_by(
        self, session: AsyncSession, column: sa.SQLColumnExpression[typing.Any], *, size: int = 1000
    ) -> typing.AsyncIterator[M]:
        async for batch in self.batches_by(session, column, size=size):
            for row in batch:
                yield row


def _decoder_for_statement(statement: AnyStatement) -> ResultDecoder[typing.Any]:
    columns = tuple(getattr(statement, "exported_columns", ()))
    if not columns:
        return RowDecoder(assemble=tuple)
    return typing.cast(ResultDecoder[typing.Any], decoder_for(columns))


@typing.overload
def query[M: DeclarativeBase](model: type[M], /) -> EntityQuery[M]: ...


@typing.overload
def query[Q: EntityQuery[typing.Any]](query_class: type[Q], /) -> Q: ...


@typing.overload
def query(statement: AnyStatement, /) -> Query[typing.Any]: ...


def query(target: type[DeclarativeBase | EntityQuery[typing.Any]] | AnyStatement, /) -> typing.Any:
    """Open a model-bound, query-class-bound or model-free query without binding an execution session."""
    if isinstance(target, type) and issubclass(target, EntityQuery):
        return target.for_model()
    if isinstance(target, type):
        return EntityQuery.for_model(target)
    if not isinstance(target, (sa.Select, sa.Insert, sa.Update, sa.Delete)):
        raise TypeError(
            "query() expects an ORM model, an EntityQuery subclass or a SQLAlchemy SELECT/INSERT/UPDATE/DELETE"
        )
    return Query(_statement=target, decoder=_decoder_for_statement(target))
