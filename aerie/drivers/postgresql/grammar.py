from aerie.drivers.base import BaseGrammar
from aerie.schema import types
from aerie.schema.structure import Column


class PostgresGrammar(BaseGrammar):
    support_replace_view = True

    def get_list_databases_sql(self) -> str:
        return "SELECT datname AS name FROM pg_database"

    def get_list_sequences_sql(self, schema_name: str = "public") -> str:
        return (
            "SELECT sequence_name AS name, sequence_schema AS schema "
            "FROM information_schema.sequences "
            "WHERE sequence_schema = '{schema_name}' "
            "AND sequence_schema != 'information_schema'"
        ).format(schema_name=schema_name)

    def get_list_views_sql(self, schema: str = "public") -> str:
        return (
            "SELECT quote_ident(table_name) AS name, "
            "view_definition AS sql "
            "FROM information_schema.views "
            "WHERE view_definition IS NOT NULL "
            "AND table_schema = '{schema_name}'"
        ).format(schema_name=schema)

    def get_list_tables_sql(self, schema: str = "public") -> str:
        return (
            "SELECT quote_ident(table_name) AS name, "
            "table_schema AS schema "
            "FROM information_schema.tables "
            "WHERE table_schema NOT LIKE 'pg\_%' "
            "AND table_schema = '{schema_name}' "
            "AND table_name != 'geometry_columns' "
            "AND table_name != 'spatial_ref_sys' "
            "AND table_type != 'VIEW'"
        ).format(schema_name=schema)

    def _type_for_smallinteger(self, column: Column[types.SmallInteger]) -> str:
        if column.type.autoincrement:
            return "SERIAL"
        return "SMALLINT"

    def _type_for_integer(self, column: Column[types.Integer]) -> str:
        if column.type.autoincrement:
            return "SERIAL"
        return "INTEGER"

    def _type_for_biginteger(self, column: Column[types.BigInteger]) -> str:
        if column.type.autoincrement:
            return "BIGSERIAL"
        return "BIGINT"

    def _type_for_binary(self, column: Column[types.Binary]) -> str:
        return "BYTEA"

    def _type_for_datetime(self, column: Column[types.DateTime]) -> str:
        if column.type.timezone:
            return "TIMESTAMP WITH TIME ZONE"
        return "TIMESTAMP"

    def _type_for_json(self, column: Column[types.JSON]) -> str:
        return "JSONB"

    def _type_for_array(self, column: Column[types.Array]) -> str:
        return "%s[]" % self.get_db_type_for(column)

    def _type_for_ipaddress(self, column: Column[types.IPAddress]) -> str:
        return "INET"

    def _type_for_macaddress(self, column: Column[types.MACAddress]) -> str:
        return "MACADDR"

    def _type_for_interval(self, column: Column[types.Interval]) -> str:
        return "INTERVAL"

    def _type_for_enum(self, column: Column[types.Enum]) -> str:
        return 'VARCHAR(255) CHECK ("%s" IN (%s))' % (
            column.name,
            ", ".join(map(self.quote, column.type.values)),
        )
