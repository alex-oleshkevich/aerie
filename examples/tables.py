import asyncio
import os
import sqlalchemy as sa
from sqlalchemy import insert, select

from aerie import metadata
from aerie.database import Aerie

DATABASE_URL = os.environ.get('DATABASE_URL', 'sqlite+aiosqlite:///:memory:')

users = sa.Table(
    'users',
    metadata,
    sa.Column(sa.Integer, name='id', primary_key=True),
    sa.Column(sa.String, name='name'),
)


async def main():
    db = Aerie(DATABASE_URL)

    # create tables
    await db.drop_tables()
    await db.create_tables()

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

        stmt = select(users).where(users.c.id == 2)
        result = await tx.execute(stmt)
        user_id2 = result.one()
        print('User with ID 2 has name: %s' % user_id2.name)


if __name__ == '__main__':
    asyncio.run(main())
