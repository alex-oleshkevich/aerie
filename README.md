# Aerie

A wrapper around SQLAlchemy made to support asynchronous workloads.

[Aerie](https://baldursgate.fandom.com/wiki/Aerie) - is an [avariel](https://baldursgate.fandom.com/wiki/Elf#Avariel)
(or winged elf) from Baldur's Gate II game.

![PyPI](https://img.shields.io/pypi/v/aerie)
![GitHub Workflow Status](https://img.shields.io/github/workflow/status/alex-oleshkevich/aerie/Lint)
![GitHub](https://img.shields.io/github/license/alex-oleshkevich/aerie)
![Libraries.io dependency status for latest release](https://img.shields.io/librariesio/release/pypi/aerie)
![PyPI - Downloads](https://img.shields.io/pypi/dm/aerie)
![GitHub Release Date](https://img.shields.io/github/release-date/alex-oleshkevich/aerie)
![Lines of code](https://img.shields.io/tokei/lines/github/alex-oleshkevich/aerie)

## Installation

Install `aerie` using PIP or poetry:

```bash
pip install aerie[postgresql]
# or
poetry add aerie[postgresql]
```

For SQLite use "sqlite" extra. To install all drivers use "full" extra.

## Features

- full async/await support
- plain SQL with bound params
- SQLAlchemy query builders support
- SQLAlchemy ORM support
- pagination

## TODO

* simplify column definition: `sa.Column(sa.Integer)` -> `models.IntergerField()`
* integrate with Alembic CLI

## Quick start

See example application in [examples/](examples/) directory of this repository.

## Usage

As this library is based on [SQLAlchemy](https://docs.sqlalchemy.org/en/14/index.html), it is strictly recommended
getting yourself familiar with it.

A general usage is:

* create an instance of Aerie
* define ORM models
* create tables in the database (or, preferably, use Alembic migrations)
* obtain a session and perform database queries

### Aerie instance

Create an instance of Aerie class and pass a connection string to it:

```python
from aerie import Aerie

db = Aerie('sqlite+aiosqlite:///tmp/database.sqlite2')
# or
db = Aerie('postgresql+asyncpg://postgres:postgres@localhost/aerie')
```

> You need appropriate driver installed. Add "aiosqlite" for SQLite support, or add "asyncpg" for PostreSQL support.

### Raw SQL queries

At this step Aerie is ready to work. Create a new transaction and execute any query you need.

```python
from sqlalchemy.sql import text

# assuming "users" table exists
sql = text('select * from users where user_id = :user_id')
rows = await db.query(sql, {'user_id': 1}).all()
```

Full listing [examples/raw_sql.py](examples/raw_sql.py)

### Using query builder

Sure, you are not limited to plain SQL. SQLAlchemy query builders also supported (because Aerie is a tiny layer on top
of the SQLAlchemy)

```python
from sqlalchemy.sql import text
import sqlalchemy as sa
from aerie import metadata

users = sa.Table(
    'users', metadata,
    sa.Column(sa.Integer, name='id', primary_key=True),
    sa.Column(sa.String, name='name'),
)

# create tables
await db.schema.create_tables()

stmt = select(users).where(users.c.id == 2)
rows = await db.query(stmt).all()
```

Full listing [examples/tables.py](examples/tables.py)

### Using ORM models and sessions

Another option to low-level query builder are ORM models. Aerie provides `aerie.Model` class that you should extend to
create your model.

```python
from aerie import Base
import sqlalchemy as sa


class User(Base):
    __tablename__ = 'users'

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)


# create tables
await db.schema.create_tables()

async with db.session() as session:
    session.add_all([
        User(id=1, name='One'),
        User(id=2, name='Two'),
        User(id=3, name='Three'),
    ])
    await session.flush()

    # get first user in the row set
    user = await session.query(User).first()
```

> Make sure the module with models is imported before you create tables.
> Otherwise they will not be added to the `metadata` and, thus, not created.

Full listing [examples/orm.py](examples/orm.py)

### Pagination

Aerie's DbSession ships with pagination utilities out of the box. When you need to paginate a query just
call `DbSession.paginate` method.

```python
async with db.session() as session:
    page = await session.query(User).paginate(page=1, page_size=10)

    for user in page:
        print(user)

    print('Next page: %s' % page.next_page)
    print('Previous page: %s' % page.previous_page)
    print('Displaying items: %s - %s' % (page.start_index, page.end_index))
```

The page object has more helper attributes:

| Property      | Type | Description                                                                                              |
|---------------|------|----------------------------------------------------------------------------------------------------------|
| total_pages   | int  | Total pages in the row set.                                                                              |
| has_next      | bool | Test if the next page is available.                                                                      |
| has_previous  | bool | Test if the previous page is available.                                                                  |
| has_other     | bool | Test if there are another pages except current one.                                                      |
| next_page     | int  | Next page number. Always returns an integer. If there is no more pages the current page number returned. |
| previous_page | int  | Previous page number. Always returns an integer. If there is no previous page, the number 1 returned.    |
| start_index   | int  | The 1-based index of the first item on this page.                                                        |
| end_index     | int  | The 1-based index of the last item on this page.                                                         |
| total_rows    | int  | Total rows in result set.                                                                                |

## Alembic migrations

Alembic usage is well documented in the official
docs: [Using Asyncio with Alembic](https://alembic.sqlalchemy.org/en/latest/cookbook.html#using-asyncio-with-alembic)

Note, you need to use `aerie.metadata` when you configure `target_metadata` option:

```python
# migrations/env.py

from aerie import metadata

target_metadata = metadata
```

Also, don't forget to import all your models in Alembic's `env.py` file so their structure is fully loaded and no models
forgotten.

## Shared instances

You can configure Aerie to populate Aerie.instances class-level variable, so you can access database instances from
anywhere of your code. For that, just pass `name` argument to Aerie constructor.

```python
# migrations/env.py

from aerie import Aerie

db = Aerie(name='shared', ...)

# other file
db = Aerie.get_instance('shared')
```

> Note, instances without name cannot be shared.
