from aerie.database import Database


class User: ...


db = Database('psql://')

# INSERT

await db.insert('users', dict(id=1, name=2)).returning('id').on_conflict(
    lambda builder: builder.on_column('id').do_nothing(),
)
await db.insert('users', dict(id=1, name=2)).returning('id').on_conflict(
    lambda builder: builder.on_constraint('uniq_idx').do_update(
        set=dict(),
        where='id = 1',
    )
)
await db.insert(
    'users',
    values=dict(id=1, name=2),
    returning=['id'],
    on_conflict=lambda b: b.on_column('id').do_nothing()
)

# SELECT

# SELECT: ORDER BY
db.select('users').order_by(
    'id', '-name', lambda o: o.by('name').nulls_last().desc(),
)
db.select(
    table='users',
    columns=['id', 'name'],
    order_by=['-name'],
)

await db.insert_from_select('users',
                            db.select('users', 'id', 'name').where(
                                name__like='23'))
await db.delete('users', id=1)
await db.update('users').set(id=1, name=2)

user = await db.select(
    'users', 'id', 'name', full='max(id)'
).where(id=1).having(name=2).group_by('id').order_by('name', 'desc').limit(
    1).offset(2).map_to(User).execute()

address = await db.select('users as u').inner_join('addresses as a',
                                                   on='a.user_id = u.id')

await db.select('users', fn.Max('id')).where(F('id') >= 1)

db.schema('users', dict(
    model=User,
    columns=dict(
        id=db.columns.integer(primary_key=True, autoincrement=True,
                              unique=True),
        name=db.columns.string(),
    ),
    indexes=[
        Index('name'),
    ]
))


class UserRepo:
    async def one(self, *where): ...

    async def all(self, *where): ...

    async def first(self, *where): ...

    async def last(self, *where): ...

    async def add(self, obj): ...

    async def remove(self, obj): ...
