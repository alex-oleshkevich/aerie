"""INSERT and UPSERT against a session.

These are session-scoped writes with no query shape to build on, so they are plain
functions taking the session first.

PostgreSQL only: the conflict handling compiles to `INSERT ... ON CONFLICT`.
"""

import typing

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase

OnConflict = typing.Literal["raise", "nothing", "set"]


def column_names(model_class: type[DeclarativeBase]) -> dict[str, str]:
    """Map ORM attribute names onto the column names the database knows them by.

    They differ whenever `mapped_column` was given an explicit name, and `excluded`
    plus the conflict index are keyed by the column name, not the attribute.
    """
    return {attr.key: attr.columns[0].key for attr in sa.inspect(model_class).column_attrs}


def onupdate_values(model_class: type[DeclarativeBase], skip: set[str]) -> dict[str, typing.Any]:
    """Resolve the Python-side `onupdate` defaults that an ON CONFLICT branch skips.

    SQLAlchemy applies these on INSERT and UPDATE statements it builds itself, but
    not inside DO UPDATE, so a timestamp like `updated_at` would keep its insert value.
    """
    resolved: dict[str, typing.Any] = {}
    for column in sa.inspect(model_class).persist_selectable.columns:
        default = column.onupdate
        if default is None or column.key in skip:
            continue
        if default.is_callable:
            resolved[column.key] = typing.cast(typing.Any, default.arg)(None)
        else:
            resolved[column.key] = typing.cast(typing.Any, default.arg)
    return resolved


@typing.overload
async def insert[M: DeclarativeBase](
    session: AsyncSession,
    model_class: type[M],
    values: dict[str, typing.Any],
    *,
    on_conflict: typing.Literal["raise", "set"] = ...,
    conflict_target: list[str] | None = ...,
    index_where: sa.ColumnElement[bool] | None = ...,
    replacements: typing.Iterable[str] | None = ...,
) -> M: ...


@typing.overload
async def insert[M: DeclarativeBase](
    session: AsyncSession,
    model_class: type[M],
    values: dict[str, typing.Any],
    *,
    on_conflict: typing.Literal["nothing"],
    conflict_target: list[str],
    index_where: sa.ColumnElement[bool] | None = ...,
    replacements: None = ...,
) -> M | None: ...


async def insert[M: DeclarativeBase](
    session: AsyncSession,
    model_class: type[M],
    values: dict[str, typing.Any],
    *,
    on_conflict: OnConflict = "raise",
    conflict_target: list[str] | None = None,
    index_where: sa.ColumnElement[bool] | None = None,
    replacements: typing.Iterable[str] | None = None,
) -> M | None:
    rows = await insert_many(
        session,
        model_class,
        [values],
        on_conflict=on_conflict,
        conflict_target=conflict_target,
        index_where=index_where,
        replacements=replacements,
    )
    return rows[0] if rows else None


async def insert_many[M: DeclarativeBase](
    session: AsyncSession,
    model_class: type[M],
    values: list[dict[str, typing.Any]],
    *,
    on_conflict: OnConflict = "raise",
    conflict_target: list[str] | None = None,
    index_where: sa.ColumnElement[bool] | None = None,
    replacements: typing.Iterable[str] | None = None,
) -> typing.Sequence[M]:
    if not values:
        return []

    # RETURNING hands back rows the session may already hold; without this the
    # identity map wins and the caller reads pre-write values.
    fresh = {"populate_existing": True}

    if on_conflict == "raise":
        stmt = sa.insert(model_class).values(values).returning(model_class)
        return (await session.scalars(stmt.execution_options(**fresh))).all()

    if not conflict_target:
        raise ValueError(f"on_conflict={on_conflict!r} requires conflict_target")

    columns = column_names(model_class)

    conflict_columns = [columns.get(name, name) for name in conflict_target]
    pg_stmt = pg_insert(model_class).values(values)
    if on_conflict == "nothing":
        pg_stmt = pg_stmt.on_conflict_do_nothing(
            index_elements=conflict_columns,
            index_where=index_where,
        )
    else:
        if replacements is None:
            replacements = [name for name in values[0] if name not in conflict_target]
        set_ = {columns.get(name, name): pg_stmt.excluded[columns.get(name, name)] for name in replacements}
        if not set_:
            raise ValueError("on_conflict='set' has no columns to update")
        set_.update(onupdate_values(model_class, skip=set(set_) | set(conflict_columns)))
        pg_stmt = pg_stmt.on_conflict_do_update(
            index_elements=conflict_columns,
            index_where=index_where,
            set_=set_,
        )

    return (await session.scalars(pg_stmt.returning(model_class).execution_options(**fresh))).all()


async def upsert[M: DeclarativeBase](
    session: AsyncSession,
    model_class: type[M],
    values: dict[str, typing.Any],
    *,
    conflict_target: list[str],
    index_where: sa.ColumnElement[bool] | None = None,
    replacements: typing.Iterable[str] | None = None,
) -> M:
    return await insert(
        session,
        model_class,
        values,
        on_conflict="set",
        conflict_target=conflict_target,
        replacements=replacements,
        index_where=index_where,
    )


async def upsert_many[M: DeclarativeBase](
    session: AsyncSession,
    model_class: type[M],
    values: list[dict[str, typing.Any]],
    *,
    conflict_target: list[str],
    index_where: sa.ColumnElement[bool] | None = None,
    replacements: typing.Iterable[str] | None = None,
) -> typing.Sequence[M]:
    return await insert_many(
        session,
        model_class,
        values,
        on_conflict="set",
        conflict_target=conflict_target,
        replacements=replacements,
        index_where=index_where,
    )
