---
name: aerie
description: >-
  Conventions for the aerie async SQLAlchemy layer: the query entry point,
  column alias naming, relationship loading, and PostgreSQL-only constraints. Invoke
  whenever task involves any interaction with aerie or the models and queries
  built on it — writing models, writing queries, adding columns, reviewing database
  code, or debugging MissingGreenlet.
---

# aerie

Async SQLAlchemy toolkit for PostgreSQL. Models are plain ORM objects, `query()` opens
a typed session-free query plan, and result shape drives static typing.

**Two rules govern everything else: models never query themselves, and nothing loads
lazily.** The rest of this document follows from those.

## Non-negotiables

- **Models never query.** There is no `User.query`, no `User.find()`, no manager
  attribute. Every ORM read starts from `query(Model)`; pass the session to its terminal.
- **Never trigger a lazy load.** On an `AsyncSession` a lazy load raises
  `MissingGreenlet`, not a slow query. Preload with SQLAlchemy's `options()` and
  `selectinload()`/`joinedload()`, or reach for
  `await obj.awaitable_attrs.rel`. Entity queries carry `raiseload("*")` so the failure
  names the attribute -- but it stops *this query* lazy-loading, it does not un-load an
  instance the session already holds. `raiseload` is not a security boundary.
- **One session per task.** `AsyncSession` is not safe across concurrently running
  tasks. Never share one across `asyncio.gather()`; give each task its own.
- **No wrapper around the session.** There is deliberately no repository object. A
  session-scoped concern -- tenant scoping, auditing, query counting -- belongs on
  SQLAlchemy's own extension points (`do_orm_execute`, `with_loader_criteria`,
  `Session.info`), not on a facade that would forward most of its methods anyway.
  `with_loader_criteria` in particular reaches eager loads, which a hand-applied
  `.where()` on the outer query does not -- that difference is a tenant leak.
- **PostgreSQL only.** Columns use `JSONB`/`ARRAY`/`TSVECTOR` and writes compile to
  `INSERT ... ON CONFLICT`. Do not add code paths for other dialects.

## Entry point

`query(Model)` opens a model-bound query plan. Passing a SQLAlchemy `Select`, `Insert`,
`Update`, or `Delete` opens a model-free plan. Neither retains a session; execution
terminals receive one explicitly.

```python
from aerie import query, upsert
from sqlalchemy.orm import selectinload

async with database.session() as session:  # DatabaseManager
    posts = await query(Post).where(Post.published).order_by(Post.title).all(session)
    one = await query(Post).pk(post_id).one_or_none(session)
```

Every `database.session()` call creates a fresh session, including nested calls. If
multiple queries need to share a session, pass that session explicitly.

Persistence is the session's: `session.add`, `flush`, `commit`, `refresh`,
`begin()`, `begin_nested()`. The package adds only what SQLAlchemy lacks:

```python
await upsert(session, Tag, {"slug": "py", "label": "Python"}, conflict_target=["slug"])
await insert_many(session, Tag, [...])
```

## Queries

Builders are plain `def` and return a new query — they only assemble a statement.
Only terminals are `async def`. Queries are immutable, so a partially built query is
safe to store and reuse.

- **Build:** `where`, `when`, `when_not`, `join`, `left_join`, `order_by`, `group_by`,
  `having`, `distinct`, `limit`, `offset`, `lock`, `options`
- **Compose:** `pipe(fn, *args)` hands the query to a function and returns whatever
  that function returns — so a pipe may change result shape, not just filters
- **Escape:** `.statement` for the raw `Select` including loader options,
  `.transform(fn)` for an arbitrary statement change that preserves the declared
  result type
- **Read:** `all(session)`, `first(session)`, `first_or_raise(session)`, `one(session)`,
  `one_or_none(session)`, `exists(session)`, `count(session)`, `sum(session, expression)`,
  `avg(session, expression)`, `min(session, expression)`, `max(session, expression)`
- **Stream:** `batches(session, size=)` / `iter(session, batch_size=)` use a server-side
  cursor; `batches_by(session, column)` / `iter_by(session, column)` use keyset paging
  and hold no cursor between batches -- prefer keyset for long-running jobs
  between batches — prefer keyset for long-running jobs

Reusable filters are functions, not a Specification class:

```python
def published(query: EntityQuery[Post]) -> EntityQuery[Post]:
    return query.where(Post.published)


recent = await query(Post).pipe(published).order_by(Post.created_at.desc()).all(session)
```

A model's own vocabulary fits a query class: subclass `EntityQuery[Model]`, return
`typing.Self` from builders, and open it with `query(PostQuery)`. Builders keep the
subclass, so the methods chain:

```python
class PostQuery(EntityQuery[Post]):
    def published(self) -> typing.Self:
        return self.where(Post.published)


recent = await query(PostQuery).published().order_by(Post.created_at.desc()).all(session)
```

### Aggregates and mutations follow the whole query

`sum`/`avg`/`min`/`max` add the aggregand to the query's own target list, so LIMIT,
OFFSET and DISTINCT keep meaning what they meant. A `group_by()` query is refused --
the aggregand is not itself grouped, so the database would reject it.

`update()`/`delete()` project the primary key through the query and match on it, so a
join or a LIMIT restricts the write exactly as it restricts the SELECT.

`options(joinedload(...))` on a collection requires `.unique()` before materializing,
because the JOIN multiplies entity rows. Streaming a joined collection still cannot
deduplicate; use `options(selectinload(...))` to stream.

### count() is bounded on purpose

`await query(Post).limit(100).count(session)` returns at most 100. It counts what the query
would return. When the unbounded total is wanted — a paginator — clear the query's
ordering and bounds first: `await query(Post).order_by(None).limit(None).offset(None).count(session)`.

### shape()

`shape(Target)` calls `Target(*row)` for the columns selected by the caller. It does not
infer columns, inspect dataclass fields, or add joins; use `join()` and `select()` first.

## Column aliases

Import from `aerie.columns` rather than repeating `mapped_column(...)`.

- **`IntPk`, `BigIntPk`** — autoincrementing integer primary keys
- **`UUIDPk`** — uuid7 primary key: time-ordered, so inserts append to the index
  instead of scattering. Encodes creation time.
- **`RandomUUIDPk`** — uuid4 primary key, for public ids that must not leak timing
- **`ZeroInt`, `EmptyText`, `FalseBool`** — NOT NULL with the type's zero value
- **`AwareDateTime`** — timezone-aware timestamp; never store naive datetimes
- **`AwareDate`, `Duration`** — plain date, and `timedelta`
- **`AutoCreatedAt`, `AutoUpdatedAt`** — timestamps populated on insert and update
- **`JSONDict`, `JSONList`** — JSONB object and array
- **`TextArray`** — native Postgres `text[]`; prefer over JSONB for tag lists because
  it takes a GIN index and answers `@>` / `&&` directly
- **`DeletedAt`** — nullable soft-delete marker, indexed
- **`Slug`** — URL-safe identifier, indexed
- **`Email`** — `String(320)`, the RFC 5321 maximum
- **`Url`, `IPAddress`, `SearchVector`, `TimeRange`** — text, `INET`, `TSVECTOR`,
  `TSTZRANGE`

Mixins: `WithTimestamps` adds `created_at`/`updated_at`; `WithUUIDKey` adds `uuid`.

## Naming convention

Two families of column alias, and the family determines the shape of the name.

**Structural** aliases say what the column *is*. Read `<Qualifier><Type>`, with the
type last so related names sort together: `ZeroInt`, `EmptyText`, `FalseBool`,
`AwareDateTime`, `RandomUUIDPk`. The qualifier names whatever is not obvious from the
type — the default (`Zero`, `Empty`, `False`), the semantics (`Aware`), or how the
value is produced (`Auto`, `Random`). `Pk` is the single suffix, because a primary key
is a role rather than a qualifier.

**Semantic** aliases say what the column *means* and deliberately omit the type:
`Slug`, `Email`, `Url`, `DeletedAt`. Storage is an implementation detail free to change
(`Text` → `CITEXT`) without renaming every model that uses it.

Elsewhere in the package:

- **Modules** — lowercase noun for the concept they own: `querying`, `decoders`,
  `writes`, `paging`. Not `utils`, `helpers`, or `common`.
- **Query classes** — `<Shape>Query`: `EntityQuery`, `ScalarQuery`, `TupleQuery`.
  Only add one when the *result shape* differs; JOIN and GROUP BY change SQL, not
  shape, so they get methods rather than classes.
- **Terminals** — name the cardinality returned: `all`, `first`, `one`,
  `one_or_none`, `count`. A terminal that raises names it: `first_or_raise`.
- **Builders** — name the SQL clause they add: `where`, `having`, `limit`.
- **Private helpers** — leading underscore, and they take the already-resolved value
  rather than re-deriving it: `_subquery_column`, `_aggregate`.
- **Tests** — `test_<module>.py`, classes `Test<Behaviour>`, methods named as the
  assertion in plain English: `test_count_respects_the_query_bound`.

### Adding a column alias

1. It must be reusable across several models. A one-off belongs beside its model.
2. Give it a generic name, not a use-case one — `Slug`, never `ArticleSlug`.
3. Do not bake `index=True` into a structural alias; it hides a schema decision from
   migration review. Semantic aliases whose whole purpose is lookup (`Slug`,
   `DeletedAt`) are the exception.
4. Do not add an alias that differs from an existing one by a single attribute. Pass
   that attribute to `mapped_column` at the model instead.
5. Changing a default here changes every model that uses it at once.

## SQLAlchemy 2.1

The package targets 2.1, where `Select` became variadic (PEP 646).

- `sa.select(Post.id)` is `Select[int]`; `sa.select(Post.id, Post.title)` is
  `Select[int, str]`. `Select[Any]` now means a select of exactly *one* column.
- For a statement of any shape, annotate `AnySelect` (from `aerie.querying`),
  which is `Select[*tuple[Any, ...]]`.
- `Row` is variadic too: use `Row[*tuple[Any, ...]]`, not `Row[Any]`.
- `Result.tuples()` is deprecated — rows unpack directly.
- `mapped_column(default=..., insert_default=...)` is now an error; they are mutually
  exclusive.
- Import `ORMOption` from `sqlalchemy.orm.interfaces`; it is not exported at
  `sqlalchemy.orm`.

## Testing

The test suite keeps its database helpers in `tests/testing.py`: it provides
`temporary_database(url, metadata)`, `create_tables`, and `drop_tables`. Nothing in
that module may take a `test_` prefix — pytest collects imported names, so a helper
called `test_database` is picked up as a test function in whatever module imports it.

The suite pattern: create tables once per session, then give each test a rollback
session via `DatabaseManager.session(force_rollback=True)`. Writes are visible inside
the test and undone on teardown, so tables are never truncated between tests.

Tests need a live PostgreSQL. `DATABASE_URL` selects it; the default matches the
docker-compose service.

## Quality gates

All three must pass before work is done:

```bash
uv run pytest --cov      # 100% branch coverage is enforced (fail_under = 100)
uv run ruff check . && uv run ruff format --check .
uv run mypy              # strict
```

Coverage is not in `addopts`, because a subset run would fail the 100% gate
spuriously. Run it explicitly.

## Application

When **writing** code against this package, apply these conventions silently — do not
narrate each rule. Where the existing code contradicts a convention, follow the code
and flag the divergence once.

When **reviewing**, cite the specific line and show the fix inline:

```
Bad:  "Consider whether this relationship might be lazily loaded, which per
       async best practices could be problematic..."
Good: "post.author is not preloaded -> InvalidRequestError at runtime.
       Add .options(selectinload(Post.author))"
```

**Before finishing any change here: every query has an explicit model or SQLAlchemy
statement, no relationship is reached without being preloaded, and every new column
alias earns its place under the naming convention above.**
