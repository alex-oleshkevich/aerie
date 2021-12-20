import sqlalchemy as sa
import typing as t

from aerie.base import Base
from aerie.collections import Collection
from aerie.queries import SelectQuery
from aerie.session import get_current_session

C = t.TypeVar('C', bound=Base)


class AutoIntegerId(Base):
    __abstract__ = True
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)


class AutoBigIntegerId(Base):
    __abstract__ = True
    id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)


class Queryable(Base):
    __abstract__ = True

    @classmethod
    def query(cls: t.Type[C]) -> SelectQuery[C]:
        return get_current_session().query(cls)

    @classmethod
    async def all(cls: t.Type[C]) -> Collection[C]:
        return await get_current_session().query(cls).all()

    @classmethod
    async def get(cls: t.Type[C], pk: t.Any, pk_column: str = 'id') -> C:
        column = getattr(cls, pk_column)
        return await get_current_session().query(cls).where(column == pk).one()

    @classmethod
    async def get_or_none(cls: t.Type[C], pk: t.Any, pk_column: str = 'id') -> t.Optional[C]:
        column = getattr(cls, pk_column)
        return await get_current_session().query(cls).where(column == pk).one_or_none()

    @classmethod
    async def create(cls: t.Type[C], **values: t.Any) -> C:
        instance = cls(**values)  # type: ignore
        await instance.save()  # type: ignore
        return instance

    @classmethod
    async def destroy(cls, *pk: t.Any, pk_column: str = 'id') -> None:
        column = getattr(cls, pk_column)
        for instance in await get_current_session().query(cls).where(column.in_(pk)).all():
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
