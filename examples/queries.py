import asyncio
import os

from aerie.database import Aerie
from aerie.fields import BigIntegerField
from aerie.models import Model
from aerie.session import DbSession

DATABASE_URL = os.environ.get('DATABASE_URL', 'sqlite:///database.sqlite')


@entity(pk=['id'], table_name='users', indexes=[
    Index(columns=['first_name', 'last_name'], unique=True),
])
class User(Model):
    id = BigIntegerField()


async def raw_sql_view(db: Aerie):
    return await db.fetch_all('select * from users')


async def users_view(request, session: DbSession):
    return session.query(User).paginate()


async def view_user_view(request, session: DbSession):
    return await session.get_by_pk(User, request.params['id'])


async def create_user_view(request, session: DbSession):
    user = User(id=1)
    await session.create(user)


async def update_user_view(request, session: DbSession):
    user = await session.get(User, 1)
    user.update(request.data)
    await session.update(user)


async def delete_user_view(request, session: DbSession):
    return await session.delete_by_pk(User, 1)


async def main():
    db = Aerie(DATABASE_URL)
    users = await db.query(User).where(User.id == 1).all()

    session = db.session()
    rows = session.query(User).one_or_none()


if __name__ == 'main':
    asyncio.run(main())
