from __future__ import annotations

import typing as t

from .structure import Schema
from ..connection import Connection


# class SchemaReflection:
#     async def reflect(self, connection: Connection) -> t.Iterable[Schema]:
#         ref = connection.get_reflection(connection)
#         schemas = await ref.get_schemas()
#         for schema in schemas:
#             schema.tables = await ref.get_tables(schema.name)
#             for table in schema:
#                 table.columns = ref.get_columns(schema.name, table.name)
#         return schemas
