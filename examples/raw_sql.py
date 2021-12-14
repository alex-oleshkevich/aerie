import asyncio
import os
import sqlalchemy as sa
from sqlalchemy import insert, text

from aerie import metadata
from aerie.database import Aerie

DATABASE_URL = os.environ.get('DATABASE_URL', 'sqlite+aiosqlite:///:memory:')

users = sa.Table(
    'users',
    metadata,
    sa.Column(sa.Integer, name='id', primary_key=True),
    sa.Column(sa.String, name='name'),
)


async def main() -> None:
    db = Aerie(DATABASE_URL)

    # create tables
    await db.schema.drop_tables()
    await db.schema.create_tables()

    # create some users
    async with db.transaction() as tx:
        stmt = insert(users).values(
            [
                {'id': 1, 'name': 'One'},
                {'id': 2, 'name': 'Two'},
                {'id': 3, 'name': 'Three'},
            ]
        )
        await tx.execute(stmt)

        result = await tx.execute(text('select * from users where id = :user_id'), {'user_id': 2})
        user_id2 = result.one()
        print('User with ID 2 has name: %s' % user_id2.name)

    # you can read db w/o transaction
    user_id3 = await db.query(text('select * from users where id = :user_id'), {'user_id': 3}).one()
    print('User with ID 3 has name: %s' % user_id3.name)


if __name__ == '__main__':
    asyncio.run(main())
