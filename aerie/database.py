from __future__ import annotations

import typing as t
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import Executable

from aerie.models import metadata as shared_metadata
from aerie.query import ExecutableQuery
from aerie.schema import Schema
from aerie.session import DbSession

_IsolationLevel = t.Literal['SERIALIZABLE', 'REPEATABLE READ', 'READ COMMITTED', 'READ UNCOMMITTED', 'AUTOCOMMIT']


class Aerie:
    instances: t.Dict[str, Aerie] = {}

    def __init__(
        self,
        url: str,
        echo: bool = False,
        isolation_level: _IsolationLevel = None,
        json_serializer: t.Callable = None,
        json_deserializer: t.Callable = None,
        metadata: MetaData = None,
        name: str = 'default',
        session_class: t.Type[DbSession] = DbSession,
        session_kwargs: t.Dict[str, t.Any] = None,
        **engine_kwargs: t.Any,
    ) -> None:
        if name in Aerie.instances:
            raise KeyError(f'Aerie instance with name "{name}" already exists. Use another name for this instance.')
        Aerie.instances[name] = self

        self.url = url
        self.metadata: MetaData = metadata or shared_metadata
        self.engine: AsyncEngine = create_async_engine(
            url,
            echo=echo,
            isolation_level=isolation_level,
            json_serializer=json_serializer,
            json_deserializer=json_deserializer,
            **engine_kwargs,
        )
        self.schema = Schema(self.engine, self.metadata)

        session_kwargs = session_kwargs or {}
        self.session: sessionmaker = sessionmaker(
            bind=self.engine,
            class_=session_class,
            expire_on_commit=False,
            **session_kwargs,
        )

    def transaction(self) -> AsyncEngine._trans_ctx:
        """Establish a new transaction."""
        return self.engine.begin()

    def query(self, stmt: Executable, params: t.Mapping = None) -> ExecutableQuery:
        return ExecutableQuery(self.engine, stmt, params)

    def get_instance(self, name: str = 'default') -> Aerie:
        if name not in Aerie.instances:
            raise KeyError(f'Aerie instance "{name}" does not exists.')
        return Aerie.instances[name]
