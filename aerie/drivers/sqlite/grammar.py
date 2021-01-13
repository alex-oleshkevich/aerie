from aerie.drivers.base import BaseGrammar
from aerie.exceptions import OperationError
from aerie.schema import operations as ops, types
from aerie.schema.structure import Column


class SqliteGrammar(BaseGrammar):
    def get_create_table_sql(self, op: ops.CreateTable) -> str:
        # test if there is a composite PK which auto increments
        # fail if so, not supported at the moment
        has_autoincrements = (
            len(
                [
                    c
                    for c in op.columns
                    if hasattr(c.type, "autoincrement") and c.type.autoincrement
                ]
            )
            > 0
        )
        pk = [c.name for c in op.columns if c.name in op.primary_key]

        if has_autoincrements and len(pk) > 1:
            raise OperationError(
                "SQLite driver does not support "
                "composite autoincrementing primary keys."
            )

        if has_autoincrements and len(pk) == 1:
            op.primary_key = []

        return super().get_create_table_sql(op)

    def get_list_tables_sql(self, schema: str = None) -> str:
        return (
            "SELECT name as name, null as schema "
            "FROM sqlite_master "
            "WHERE type = 'table' "
            "AND name != 'sqlite_sequence' "
            "AND name != 'geometry_columns' "
            "AND name != 'spatial_ref_sys' "
            "UNION ALL SELECT name, null as schema FROM sqlite_temp_master "
            "WHERE type = 'table' "
            "ORDER BY name"
        )

    def get_list_databases_sql(self) -> str:
        raise OperationError("SQLite does not support database listing.")

    def get_list_sequences_sql(self, schema: str = None) -> str:
        raise OperationError("SQLite does not support sequence listing.")

    def get_list_views_sql(self, schema: str = None) -> str:
        return (
            "SELECT name, sql "
            "FROM sqlite_master "
            "WHERE type='view' AND sql NOT NULL"
        )

    def _type_for_string(self, column: Column[types.String]) -> str:
        return "VARCHAR"

    def _type_for_smallinteger(self, column: Column[types.SmallInteger]) -> str:
        if column.type.autoincrement:
            return "INTEGER PRIMARY KEY AUTOINCREMENT"
        return "INTEGER"

    def _type_for_integer(self, column: Column[types.Integer]) -> str:
        if column.type.autoincrement:
            return "INTEGER PRIMARY KEY AUTOINCREMENT"
        return "INTEGER"

    def _type_for_biginteger(self, column: Column[types.BigInteger]) -> str:
        if column.type.autoincrement:
            return "INTEGER PRIMARY KEY AUTOINCREMENT"
        return "INTEGER"

    def _type_for_json(self, column: Column[types.JSON]) -> str:
        return "TEXT"

    def _type_for_ipaddress(self, column: Column[types.IPAddress]) -> str:
        return "VARCHAR"

    def _type_for_macaddress(self, column: Column[types.MACAddress]) -> str:
        return "VARCHAR"

    def _type_for_uuid(self, column: Column[types.UUID]) -> str:
        return "VARCHAR"

    def _type_for_decimal(self, column: Column[types.Decimal]) -> str:
        return "NUMERIC"

    def _type_for_boolean(self, column: Column[types.Boolean]) -> str:
        return "TINYINT(1)"

    def _type_for_enum(self, column: Column[types.Enum]) -> str:
        return 'VARCHAR CHECK ("%s" IN (%s))' % (
            column.name,
            ", ".join(map(self.quote, column.type.values)),
        )

    def _type_for_interval(self, column: Column[types.Interval]) -> str:
        return "INTEGER"
