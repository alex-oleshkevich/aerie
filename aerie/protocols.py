import typing as t

from sqlalchemy.sql import Select


class Executor(t.Protocol):
    async def execute(self, statement: Select, params: t.Mapping = None, *args, **kwargs) -> t.Any:
        pass
