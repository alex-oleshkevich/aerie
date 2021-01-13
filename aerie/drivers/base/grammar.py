import typing as t

from aerie.protocols import Empty
from aerie.schema import operations as ops, types
from aerie.schema.structure import Column

T = t.TypeVar("T")


class BaseGrammar:
    type_map = {
        types.SmallInteger: "_type_for_smallinteger",
        types.Integer: "_type_for_integer",
        types.BigInteger: "_type_for_biginteger",
        types.String: "_type_for_string",
        types.Text: "_type_for_text",
        types.Boolean: "_type_for_boolean",
        types.DateTime: "_type_for_datetime",
        types.Date: "_type_for_date",
        types.Time: "_type_for_time",
        types.Float: "_type_for_float",
        types.Decimal: "_type_for_decimal",
        types.Numeric: "_type_for_numeric",
        types.Binary: "_type_for_binary",
        types.JSON: "_type_for_json",
        types.UUID: "_type_for_uuid",
        types.Enum: "_type_for_enum",
        types.IPAddress: "_type_for_ipaddress",
        types.MACAddress: "_type_for_macaddress",
        types.Array: "_type_for_array",
        types.Interval: "_type_for_interval",
        types.Timestamp: "_type_for_timestamp",
    }

    support_replace_view = False

    quote_char = "'"

    def get_list_databases_sql(self) -> str:
        raise NotImplementedError()

    def get_create_table_sql(self, op: ops.CreateTable) -> str:
        columns = [
            "{name} {type}{null}{default}".format(
                name=c.name,
                type=self.get_db_type_for(c),
                null=(
                    " NULL"
                    if c.null is True
                    else " NOT NULL"
                    if c.null is False
                    else ""
                ),
                default=(" DEFAULT %s" % self.quote(c.default) if c.default else ""),
            )
            for c in op.columns
        ]

        pk = [c.name for c in op.columns if c.name in op.primary_key]

        sql = "CREATE{temporary}TABLE{if_not_exists} {name} ({columns}{pk})"
        return sql.format(
            temporary=" TEMPORARY " if op.temporary else " ",
            if_not_exists=" IF NOT EXISTS" if op.if_not_exist else "",
            name=op.table_name,
            columns=", ".join(columns),
            pk=", PRIMARY KEY (%s)" % ", ".join(pk) if len(pk) else "",
        )

    def get_list_tables_sql(self, schema: str = "public") -> str:
        raise NotImplementedError()

    def get_rename_table_sql(self, op: ops.RenameTable) -> str:
        return "ALTER TABLE {table} RENAME TO {new_name}".format(
            table=op.table_name,
            new_name=op.new_table_name,
        )

    def get_drop_table_sql(self, op: ops.DropTable) -> str:
        return "DROP TABLE {if_exists}{table}".format(
            table=op.table_name,
            if_exists="IF EXISTS " if op.if_exists else "",
        )

    def get_add_column_sql(self, op: ops.AddColumn) -> str:
        return "ALTER TABLE {table} ADD COLUMN{if_not_exists} {name} {type}{null}{default}".format(
            table=op.table_name,
            if_not_exists=" IF NOT EXISTS" if op.if_not_exists else "",
            name=op.column_name,
            type=self.get_db_type_for(
                Column(
                    name=op.column_name,
                    type=op.column_type,
                    null=op.null,
                    default=op.default,
                )
            ),
            null=(
                " NULL" if op.null is True else " NOT NULL" if op.null is False else ""
            ),
            default=(
                " DEFAULT %s" % self.quote(op.default)
                if not isinstance(op.default, Empty)
                else ""
            ),
        )

    def get_rename_column_sql(self, op: ops.RenameColumn) -> str:
        return (
            "ALTER TABLE {name}{if_exists} RENAME COLUMN {column} TO {new_name}".format(
                name=op.table_name,
                if_exists=" IF EXISTS" if op.if_exists else "",
                column=op.column_name,
                new_name=op.new_column_name,
            )
        )

    def get_drop_column_sql(self, op: ops.DropColumn) -> str:
        return "ALTER TABLE {name} DROP COLUMN{if_exists} {column}{cascade}".format(
            name=op.table_name,
            if_exists=" IF EXISTS" if op.if_exists else "",
            column=op.column_name,
            cascade=" CASCADE" if op.cascade else "",
        )

    def get_create_index_sql(self, op: ops.AddIndex) -> str:
        columns = [
            "{column}{opclass}{sort}".format(
                column=c,
                opclass=" " + op.operator_class if op.operator_class else "",
                sort=" " + op.sort.get(c).upper() if c in op.sort else "",
            )
            for c in op.columns
        ]

        return (
            "CREATE{unique} INDEX{concurrently}{if_not_exists} {name} "
            "ON {table}{using} {columns}{where}"
        ).format(
            unique=" UNIQUE" if op.unique else "",
            concurrently=" CONCURRENTLY" if op.concurrently else "",
            if_not_exists=" IF NOT EXISTS" if op.if_not_exists else "",
            name=op.name,
            table=op.table_name,
            using=" USING %s" % op.using if op.using else "",
            columns="(%s)" % ", ".join(columns),
            where=" WHERE %s" % str(op.where) if op.where else "",
        )

    def get_drop_index_sql(self, op: ops.DropIndex) -> str:
        return "DROP INDEX{concurrently}{if_exists} {name}{cascade}".format(
            concurrently=" CONCURRENTLY" if op.concurrently else "",
            if_exists=" IF EXISTS" if op.if_exists else "",
            name=op.index_name,
            cascade=" CASCADE" if op.cascade else "",
        )

    def get_create_sequence_sql(self, op: ops.CreateSequence) -> str:
        sql = (
            "CREATE{temporary}SEQUENCE{if_not_exists}{name}{increment_by}"
            "{min_value}{max_value}{start_with}{cycle}"
        )
        return sql.format(
            temporary=" TEMPORARY " if op.temporary else " ",
            if_not_exists=" IF NOT EXISTS " if op.if_not_exists else " ",
            name=op.name,
            increment_by=(
                " INCREMENT BY %s" % op.increment_by
                if op.increment_by is not None
                else ""
            ),
            min_value=(
                " MINVALUE %s" % op.min_value if op.min_value is not None else ""
            ),
            max_value=(
                " MAXVALUE %s" % op.max_value if op.max_value is not None else ""
            ),
            start_with=(
                " START WITH %s" % op.start_with if op.start_with is not None else ""
            ),
            cycle=" CYCLE" if op.cycle else "",
        )

    def get_alter_sequence_sql(self, op: ops.AlterSequence) -> str:
        return (
            "ALTER SEQUENCE {name}{increment_by}{min_value}{max_value}"
            "{start_with}{restart_with}{cycle}"
        ).format(
            name=op.name,
            increment_by=(
                " INCREMENT BY %s" % op.increment_by
                if op.increment_by is not None
                else ""
            ),
            min_value=(
                " MINVALUE %s" % op.min_value if op.min_value is not None else ""
            ),
            max_value=(
                " MAXVALUE %s" % op.max_value if op.max_value is not None else ""
            ),
            start_with=(
                " START WITH %s" % op.start_with if op.start_with is not None else ""
            ),
            restart_with=(
                " RESTART WITH %s" % op.restart_with
                if op.restart_with is not None
                else ""
            ),
            cycle=" CYCLE" if op.cycle else "",
        )

    def get_rename_sequence_sql(self, op: ops.RenameSequence) -> str:
        return "ALTER SEQUENCE {name} RENAME TO {new_name}".format(
            name=op.name,
            new_name=op.new_name,
        )

    def get_list_sequences_sql(self, schema_name: str = None) -> str:
        raise NotImplementedError()

    def get_drop_sequence_sql(self, op: ops.DropSequence) -> str:
        return "DROP SEQUENCE{if_exists}{name}{cascade}".format(
            if_exists=" IF EXISTS " if op.if_exists else " ",
            name=op.name,
            cascade=" CASCADE" if op.cascade else "",
        )

    def get_create_view_sql(self, op: ops.CreateView) -> str:
        columns = ""
        if op.columns:
            columns = f"({', '.join(op.columns)}) "

        replace = ""
        if self.support_replace_view:
            replace = " OR REPLACE" if op.replace else ""

        return "CREATE{replace}{temporary}VIEW {name} {columns}AS {sql}".format(
            replace=replace,
            temporary=" TEMPORARY " if op.temporary else " ",
            name=op.name,
            columns=columns,
            sql=str(op.sql),
        )

    def get_rename_view_sql(self, op: ops.RenameView) -> str:
        return "ALTER VIEW{if_exists}{name} RENAME TO {new_name}".format(
            if_exists=" IF EXISTS " if op.if_exists else " ",
            name=op.name,
            new_name=op.new_name,
        )

    def get_list_views_sql(self, schema: str = "public") -> str:
        raise NotImplementedError()

    def get_drop_view_sql(self, op: ops.DropView) -> str:
        return "DROP VIEW{if_exists}{name}{cascade}".format(
            if_exists=" IF EXISTS " if op.if_exists else " ",
            name=op.name,
            cascade=" CASCADE" if op.cascade else "",
        )

    def get_create_foreign_key_sql(self, op: ops.CreateForeignKey) -> str:
        raise NotImplementedError()

    def drop_create_foreign_key_sql(self, op: ops.DropForeignKey) -> str:
        raise NotImplementedError()

    def get_db_type_for(self, column: Column) -> str:
        method_name = self.type_map.get(column.type.__class__)
        if not method_name:
            self._raise_not_supported(column)
        return getattr(self, method_name)(column)

    def _type_for_smallinteger(self, column: Column[types.SmallInteger]) -> str:
        self._raise_not_supported(column)

    def _type_for_integer(self, column: Column[types.Integer]) -> str:
        self._raise_not_supported(column)

    def _type_for_biginteger(self, column: Column[types.BigInteger]) -> str:
        self._raise_not_supported(column)

    def _type_for_string(self, column: Column[types.String]) -> str:
        return "VARCHAR(%s)" % column.type.length

    def _type_for_text(self, column: Column[types.Text]) -> str:
        return "TEXT"

    def _type_for_boolean(self, column: Column[types.Boolean]) -> str:
        return "BOOLEAN"

    def _type_for_datetime(self, column: Column[types.DateTime]) -> str:
        return "DATETIME"

    def _type_for_date(self, column: Column[types.Date]) -> str:
        return "DATE"

    def _type_for_time(self, column: Column[types.Time]) -> str:
        return "TIME"

    def _type_for_interval(self, column: Column[types.Interval]) -> str:
        self._raise_not_supported(column)

    def _type_for_timestamp(self, column: Column[types.Timestamp]) -> str:
        return "TIMESTAMP"

    def _type_for_number(self, inner_type: str, column: Column[types.Float]) -> str:
        if column.type.precision and column.type.scale:
            return "%s(%s, %s)" % (inner_type, column.type.precision, column.type.scale)
        if column.type.precision:
            return "%s(%s)" % (inner_type, column.type.precision)
        return "%s" % inner_type

    def _type_for_numeric(self, column: Column[types.Numeric]) -> str:
        return self._type_for_number("NUMERIC", column)

    def _type_for_float(self, column: Column[types.Float]) -> str:
        return self._type_for_number("FLOAT", column)

    def _type_for_decimal(self, column: Column[types.Decimal]) -> str:
        return self._type_for_number("DECIMAL", column)

    def _type_for_binary(self, column: Column[types.Binary]) -> str:
        return "BLOB"

    def _type_for_json(self, column: Column[types.JSON]) -> str:
        self._raise_not_supported(column)

    def _type_for_uuid(self, column: Column[types.UUID]) -> str:
        return "UUID"

    def _type_for_enum(self, column: Column[types.Enum]) -> str:
        self._raise_not_supported(column)

    def _type_for_ipaddress(self, column: Column[types.IPAddress]) -> str:
        self._raise_not_supported(column)

    def _type_for_macaddress(self, column: Column[types.MACAddress]) -> str:
        self._raise_not_supported(column)

    def _type_for_array(self, column: Column[types.Array]) -> str:
        self._raise_not_supported(column)

    def _raise_not_supported(self, column: Column[types.BaseType]) -> None:
        raise NotImplementedError(
            'Grammar %s does not implement type "%s"'
            % (
                self.__class__.__name__,
                column.type.__class__.__name__,
            )
        )

    def quote(self, value: t.Any) -> t.Any:
        if value is None:
            return "NULL"
        if value is False:
            return "FALSE"
        if value is True:
            return "TRUE"

        if hasattr(value, "get_sql"):
            return value.get_sql()

        if isinstance(value, str):
            return "%s%s%s" % (self.quote_char, value, self.quote_char)
        return value
