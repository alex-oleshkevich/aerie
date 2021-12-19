import sqlalchemy as sa
import typing as t

from aerie.models import BaseModel
from aerie.queries import SelectQuery
from aerie.session import DbSession

C = t.TypeVar('C', bound=BaseModel)


class AutoIntegerId(BaseModel):
    __abstract__ = True
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)


class AutoBigIntegerId(BaseModel):
    __abstract__ = True
    id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)


class Queryable(BaseModel):
    __abstract__ = True

    @classmethod
    def query(cls: t.Type[C]) -> SelectQuery[C]:
        return DbSession.get_current_session().query(cls)

    async def save(self, commit: bool = True) -> None:
        session = DbSession.get_current_session()
        session.add(self)  # type: ignore
        if commit:
            await session.commit()
