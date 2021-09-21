import asyncio
import os

import sqlalchemy as sa

from aerie import Aerie, Model

DATABASE_URL = os.environ.get('DATABASE_URL', 'sqlite+aiosqlite:///:memory')


class User(Model):
    __tablename__ = 'users'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)

    def __str__(self) -> str:
        return self.name


async def main():
    db = Aerie(DATABASE_URL)

    # create tables
    await db.drop_tables()
    await db.create_tables()

    # create some users
    async with db.session() as session:
        session.add_all([
            User(id=1, name='One'),
            User(id=2, name='Two'),
            User(id=3, name='Three'),
        ])
        await session.flush()

        # get first user
        stmt = session.select(User)
        user = await session.first(stmt)
        print('First user: %s' % user)

        # get user by PK
        user = await session.get(User, 2)
        print('User by PK: %s' % user)

        # user by condition
        stmt = session.select(User).where(User.name == 'One')
        user = await session.one(stmt)
        print('User by where clause: %s' % user)

        # all users
        stmt = session.select(User)
        users = await session.all(stmt)
        print('All users: %s' % [str(u) for u in users])


if __name__ == '__main__':
    asyncio.run(main())
