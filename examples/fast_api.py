"""
This example requires uvicorn and fastapi.

pip install fastapi uvicorn

Run:
    uvicorn examples.fast_api:app

then open http://localhost:8000

Access http://localhost:8000 to list all users.
Access http://localhost:8000/create to create a new user.
"""
import os
import sqlalchemy as sa
import typing as t
from fastapi import Depends, FastAPI
from sqlalchemy import select

from aerie import Aerie, DbSession, Model

DATABASE_URL = os.environ.get('DATABASE_URL', 'sqlite+aiosqlite:///:memory:')

db = Aerie(DATABASE_URL)


class User(Model):
    __tablename__ = 'users'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)

    def __str__(self) -> str:
        return self.name or 'n/a'


async def db_session() -> t.AsyncGenerator[DbSession, None]:
    async with db.session() as session:
        yield session


app = FastAPI(on_startup=[db.create_tables], on_shutdown=[db.drop_tables])


@app.get("/create")
async def create_user_view(session: DbSession = Depends(db_session)) -> t.Mapping:
    count = await session.query(select(User)).count()
    user = User(id=count, name=f'User {count}')
    session.add(user)
    await session.commit()

    return {"id": user.id, 'name': user.name}


@app.get("/")
async def list_users_view(session: DbSession = Depends(db_session)) -> t.List:
    users = await session.query(select(User)).all()
    return [u.name for u in users]
