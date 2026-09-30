# aerie

Async SQLAlchemy toolkit for PostgreSQL. It gives you an engine and session manager,
typed queries that you build once and run on any session, offset and keyset
pagination, bulk INSERT and UPSERT helpers, and a set of ready-made column types.

aerie works on top of SQLAlchemy 2.1 and does not replace it. Models are ordinary
declarative models, and every query can drop down to a plain `select()`.

```
pip install aerie
pip install "aerie[fastapi]"    # DbSession dependency for FastAPI
pip install "aerie[starlette]"  # DbSessionMiddleware
```

You also need an async PostgreSQL driver, such as `psycopg[binary]` or `asyncpg`.

## Getting started

In this walkthrough you define a model, store two rows and read one back. You need a
running PostgreSQL and its connection URL.

Create `blog.py`:

```python
import asyncio

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from aerie import Base, DatabaseManager, WithTimestamps, query
from aerie.columns import IntPk


class Post(WithTimestamps, Base):
    __tablename__ = "posts"

    id: Mapped[IntPk]
    title: Mapped[str] = mapped_column(sa.String(200))
    published: Mapped[bool] = mapped_column(default=False)


async def main() -> None:
    async with DatabaseManager("postgresql+psycopg://postgres:postgres@localhost/postgres") as db:
        async with db.get_engine().begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

        async with db.session() as session:
            session.add_all([Post(title="Hello", published=True), Post(title="Draft")])
            await session.commit()

        async with db.session() as session:
            published = await query(Post).where(Post.published).all(session)
            print(published)


asyncio.run(main())
```

Run it with `python blog.py`. You will see one post:

```
[Post(pk=[1])]
```

The draft was filtered out by `.where(Post.published)`. Notice that the query was
built first and the session was passed only to `.all()`, the call that runs it.
Every aerie query works this way.

## How-to guides

### Use a session in FastAPI routes

Open the manager in the lifespan, add the middleware, and ask for `DbSession` in a
route. The middleware opens one session per request.

```python
import contextlib

import fastapi
from starlette.middleware import Middleware

from aerie import DatabaseManager, query
from aerie.ext.fastapi import DbSession
from aerie.ext.starlette import DbSessionMiddleware

database = DatabaseManager("postgresql+psycopg://...")


@contextlib.asynccontextmanager
async def lifespan(app: fastapi.FastAPI):
    async with database:
        yield


app = fastapi.FastAPI(lifespan=lifespan, middleware=[Middleware(DbSessionMiddleware, database)])


@app.get("/posts")
async def list_posts(session: DbSession) -> list[str]:
    return await query(Post).select(Post.title).all(session)
```

The middleware does not commit. Call `await session.commit()` in routes that write.

### Paginate a list

For page numbers and a total count, use `paginate()`:

```python
page = await query(Post).order_by(Post.id).paginate(session, number=2, size=20)
page.items, page.total, page.pages, page.has_next
```

For large or fast-changing tables, use keyset pagination. It reads no total and does
not slow down on deep pages. Pass `next_cursor` back as `after` to get the next page:

```python
first = await query(Post).cursor_page(session, size=20, by=[Post.id])
second = await query(Post).cursor_page(session, size=20, by=[Post.id], after=first.next_cursor)
```

The `by` columns must be unique together, or rows will be skipped or repeated.

### Load relationships

aerie never loads relationships for you. Pick a loader with `options()`, and call
`unique()` when a joined collection repeats the parent rows:

```python
from sqlalchemy.orm import joinedload, selectinload

posts = await query(Post).options(selectinload(Post.tags)).all(session)
authors = await query(Author).options(joinedload(Author.posts)).unique().all(session)
```

A relationship that was not loaded raises `MissingGreenlet` when you touch it on an
async session. That error means a loader option is missing.

### Select columns instead of entities

`select()` changes what the query returns, and the result stays typed:

```python
titles = await query(Post).select(Post.title).all(session)  # list[str]
pairs = await query(Post).select(Post.id, Post.title).all(session)  # list[tuple[int, str]]
ids = await query(Post).select(Post.id).set(session)  # set[int]
```

To build objects from the selected columns, add `shape()`. It calls the target with
the row values in order:

```python
@dataclasses.dataclass
class Card:
    title: str
    author: str


cards = await query(Post).join(Post.author).select(Post.title, Author.name).shape(Card).all(session)
```

### Add filters conditionally

`when()` adds a predicate only when the condition is true, and it does not call the
callback otherwise:

```python
posts = await (
    query(Post)
    .when(search is not None, lambda: Post.title.ilike(f"%{search}%"))
    .when_not(include_drafts, lambda: Post.published)
    .all(session)
)
```

### Update or delete the rows a query matches

```python
await query(Post).where(Post.published.is_(False)).update().set(Post.title, "Untitled").execute(session)
await query(Post).where(Post.id == post_id).delete().execute(session)
```

`inc()` and `dec()` change numeric columns in place, and `returning()` gives the
changed rows back.

### Insert or upsert in bulk

```python
from aerie import insert_many, upsert

await insert_many(session, Tag, [{"name": "python"}, {"name": "sql"}], on_conflict="nothing", conflict_target=["name"])
tag = await upsert(session, Tag, {"name": "python", "colour": "blue"}, conflict_target=["name"])
```

Both compile to `INSERT ... ON CONFLICT`.

### Roll back every test

Open the test session with `force_rollback=True`. Everything the test writes,
including commits, runs inside a savepoint that is rolled back when the context
exits.

```python
@pytest.fixture
async def session(database: DatabaseManager):
    async with database.session(force_rollback=True) as session:
        yield session
```

To make the application code under test use that same session, create the manager
with `force_session_reuse=True` in the test settings. A nested `database.session()`
then joins the session already open in the current context instead of opening a
new one.

## Reference

### Package layout

| Module | Contents |
| --- | --- |
| `aerie.manager` | `DatabaseManager`: engine lifecycle and sessions |
| `aerie.querying` | `query()`, `Query`, `EntityQuery`, `ScalarQuery`, `TupleQuery`, `ShapeQuery` |
| `aerie.mutation` | `UpdateQuery`, `DeleteQuery`, `ReturningQuery`, `MutationResult` |
| `aerie.paging` | `Page`, `CursorPage` |
| `aerie.writes` | `insert`, `insert_many`, `upsert`, `upsert_many` |
| `aerie.decoders` | `ScalarDecoder`, `RowDecoder`: turn result rows into values |
| `aerie.models` | `Base`, `ReprMixin`, `WithTimestamps`, `WithUUIDKey`, `import_model` |
| `aerie.columns` | `Annotated` column types |
| `aerie.choices` | `TextChoices`: a `StrEnum` with labels |
| `aerie.ext.fastapi` | `get_dbsession`, `DbSession` (needs the `fastapi` extra) |
| `aerie.ext.starlette` | `DbSessionMiddleware` (needs the `starlette` extra) |

`aerie` re-exports everything above except the column types and the `ext` modules.
Importing `aerie` never imports a web framework.

### `DatabaseManager`

`DatabaseManager(url, *, pool_size=15, max_overflow=2, pool_timeout=10, pool_recycle=1000,
pool_pre_ping=False, echo=False, isolation_level="READ COMMITTED",
dangerously_disable_pool=False, force_session_reuse=False)`

| Member | Description |
| --- | --- |
| `async with manager` | Creates the engine on entry and disposes it on exit. Nested entries are reference-counted. |
| `session(force_rollback=False)` | Async context manager that yields an `AsyncSession`. Sessions use `expire_on_commit=False`. |
| `get_engine()` | The running `AsyncEngine`. Raises `LookupError` outside `async with`. |
| `current_session()` | The session open in the current context, or `None`. |
| `dsn` | The URL passed to the constructor. |

### Query methods

Builders return a new query and never touch the database: `where`, `when`,
`when_not`, `join`, `left_join`, `order_by`, `group_by`, `having`, `distinct`,
`limit`, `offset`, `lock`, `options`, `unique`, `select`, `shape`, `transform`,
`pipe`.

Terminals take the session and run the query: `all`, `first`, `first_or_raise`,
`one`, `one_or_none`, `one_or_raise`, `exists`, `count`, `sum`, `avg`, `min`, `max`,
`batches`, `iter`, `paginate`, `execute`.

`EntityQuery` (from `query(Model)`) adds `pk`, `map`, `update`, `delete`,
`cursor_page`, `batches_by` and `iter_by`. `ScalarQuery` adds `set`. `sql()` returns
the compiled SQL of any query.

### Column types

| Type | Column |
| --- | --- |
| `IntPk`, `BigIntPk` | Autoincrement integer primary key |
| `UUIDPk`, `RandomUUIDPk` | UUID primary key, defaults to `uuid7()` or `uuid4()` |
| `AutoCreatedAt`, `AutoUpdatedAt` | Timezone-aware timestamps set on insert and on update |
| `DeletedAt` | Nullable, indexed timestamp for soft deletes |
| `AwareDateTime`, `DateTime`, `Date`, `Duration` | Date and time columns |
| `ZeroInt`, `EmptyText`, `FalseBool` | Columns with a server default of `0`, `''` or `false` |
| `JSONDict`, `JSONList`, `TextArray` | `jsonb` and `text[]` columns that default to empty |
| `Slug`, `Email`, `Url`, `IPAddress` | Indexed `varchar(255)`, `varchar(320)`, `text`, `inet` |
| `SearchVector`, `TimeRange` | `tsvector` and `tstzrange` |

### `Base`

`Base` is an abstract declarative base with `AsyncAttrs` and a readable `__repr__`.
Its metadata names constraints like this:

| Constraint | Name |
| --- | --- |
| Primary key | `<table>_pkey` |
| Unique | `<table>_<columns>_key` |
| Foreign key | `<table>_<columns>_fkey` |
| Index | `ix_<table>_<column>` |
| Check | the constraint's own name |

`StrEnum` annotations map to a PostgreSQL enum of the member values.
`populate(values, exclude=None)` sets attributes from a dict and raises
`AttributeError` for names the model does not have.

## Design notes

**Queries are not bound to a session.** `query(Post).where(...)` is only a plan, and
the session arrives at the terminal. A query can be built in one place, passed
around, and run on whichever session the caller holds, including a test's rollback
session. Nothing has to track which session an object belongs to, which is a common
source of bugs in async code.

**Models do not query themselves.** There is no `Post.objects` or `Post.get()`. Reads
start from `query(Model)` and writes go through the session, so a model stays a
plain description of a table.

**Loading is explicit.** Lazy loading would need I/O inside attribute access, which
an async session cannot do, so SQLAlchemy raises `MissingGreenlet` instead. aerie
does not guess loaders for you. You choose `selectinload()` or `joinedload()`, and a
missing choice fails loudly rather than running a hidden query.

**PostgreSQL only.** The column types use `jsonb`, `inet`, `tsvector` and ranges, and
the write helpers compile to `INSERT ... ON CONFLICT`. Supporting other databases
would mean dropping those features or hiding them behind flags.

## License

MIT
