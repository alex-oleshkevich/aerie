import sqlalchemy as sa
import typing as t
from sqlalchemy import Boolean, inspect
from sqlalchemy.sql import ColumnElement

from aerie.base import Base
from aerie.collections import Collection
from aerie.queries import SelectQuery
from aerie.session import get_current_session

C = t.TypeVar('C', bound='BaseModel')


class AutoIntegerId:
    __abstract__ = True
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)


class AutoBigIntegerId:
    __abstract__ = True
    id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)


class _QueryProperty(t.Generic[C]):
    def __get__(self, obj: t.Optional[C], type: t.Type[C]) -> SelectQuery[C]:
        return get_current_session().query(type)


class BaseModel(Base):
    __abstract__ = True

    @classmethod
    def query(cls: t.Type[C]) -> SelectQuery[C]:
        return get_current_session().query(cls)

    @classmethod
    async def first(cls: t.Type[C]) -> t.Optional[C]:
        return await cls.query().first()

    @classmethod
    async def all(cls: t.Type[C]) -> Collection[C]:
        return await cls.query().all()

    @classmethod
    async def get(cls: t.Type[C], pk: t.Any, pk_column: str = 'id') -> C:
        column = getattr(cls, pk_column)
        return await cls.query().where(column == pk).one()

    @classmethod
    async def get_or_none(cls: t.Type[C], pk: t.Any, pk_column: str = 'id') -> t.Optional[C]:
        column = getattr(cls, pk_column)
        return await cls.query().where(column == pk).one_or_none()

    @classmethod
    async def get_or_create(
        cls: t.Type[C],
        where: ColumnElement[Boolean],
        values: t.Mapping[str, t.Any],
        autoflush: bool = True,
        autocommit: bool = False,
    ) -> C:
        instance = await cls.query().where(where).one_or_none()
        if not instance:
            instance = await cls.create(autoflush=autoflush, autocommit=autocommit, **values)
        return instance

    @classmethod
    async def create(cls: t.Type[C], autoflush: bool = True, autocommit: bool = False, **values: t.Any) -> C:
        instance = cls(**values)  # type: ignore
        get_current_session().add(instance)
        if autoflush:
            await get_current_session().flush([instance])
        await instance.save(commit=autocommit)  # type: ignore
        return instance

    @classmethod
    async def destroy(cls, *pk: t.Any, pk_column: str = 'id') -> None:
        column = getattr(cls, pk_column)
        for instance in await cls.query().where(column.in_(pk)).all():
            await instance.delete()

    async def save(self, commit: bool = True) -> None:
        session = get_current_session()
        session.add(self)  # type: ignore
        if commit:
            await session.commit()

    async def delete(self, commit: bool = True) -> None:
        session = get_current_session()
        await session.delete(self)
        if commit:
            await session.commit()

    async def refresh(self) -> None:
        session = get_current_session()
        await session.refresh(self)

    def __repr__(self) -> str:
        identity = inspect(self).identity
        if identity is None:
            pk = f"transient {id(self)}"
        elif len(identity) > 1:
            pk = f'pk={identity}'
        else:
            pk = ", ".join(str(value) for value in identity)
            pk = 'pk=' + pk

        return f"<{type(self).__name__}: {pk}>"
