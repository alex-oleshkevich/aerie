from __future__ import annotations

import datetime
import decimal as decimallib
import inspect
import typing as t
import uuid as uuidlib
from contextlib import contextmanager

from aerie.connection import Connection
from aerie.drivers.base import BaseGrammar
from aerie.protocols import StrLike
from aerie.schema import operations as ops, types
from aerie.schema.structure import Column, Sequence, Table, View
from aerie.terms import Now, OrderBy


class _AddsIndices:
    table_name: str
    _ops: t.List[ops.Operation]

    def add_index(
        self,
        columns: t.Union[str, t.Iterable[str]],
        unique: bool = False,
        sort: t.Union[str, t.Dict[str, t.Union[str, str]], OrderBy] = None,
        where: str = None,
        using: str = None,
        operator_class: str = None,
        name: str = None,
        if_not_exists: bool = None,
    ) -> None:
        self._ops.append(
            ops.AddIndex(
                table_name=self.table_name,
                columns=columns,
                name=name,
                sort=sort,
                where=where,
                operator_class=operator_class,
                unique=unique,
                using=using,
                if_not_exists=if_not_exists,
            )
        )


class _AddColumns:
    table_name: str
    _ops: t.List[ops.Operation]
    add_column: t.Callable

    def increments(self, column_name: str = "id") -> None:
        self.add_column(
            column_name=column_name,
            column_type=types.BigInteger(autoincrement=True),
            primary_key=True,
        )

    def timestamps(
        self,
        tz: bool = False,
        created_at_name: str = "created_at",
        updated_at_name: str = "updated_at",
    ):
        self.add_column(created_at_name, types.DateTime(tz), default=Now())
        self.add_column(updated_at_name, types.DateTime(tz), default=Now())

    def string(
        self,
        name: str,
        length: int = 255,
        default: str = None,
        null: bool = False,
        index: bool = False,
        unique: bool = False,
        primary_key: bool = False,
    ):
        self.add_column(
            column_name=name,
            column_type=types.String(length),
            default=default,
            null=null,
            index=index,
            unique=unique,
            primary_key=primary_key,
        )

    def text(
        self,
        name: str,
        default: str = None,
        null: bool = False,
        index: bool = False,
        unique: bool = False,
        primary_key: bool = False,
    ):
        self.add_column(
            column_name=name,
            column_type=types.Text(),
            default=default,
            null=null,
            index=index,
            unique=unique,
            primary_key=primary_key,
        )

    def big_integer(
        self,
        name: str,
        default: int = None,
        null: bool = False,
        index: bool = False,
        unique: bool = False,
        primary_key: bool = False,
        autoincrement: bool = False,
    ):
        self.add_column(
            column_name=name,
            column_type=types.BigInteger(autoincrement=autoincrement),
            default=default,
            null=null,
            index=index,
            unique=unique,
            primary_key=primary_key,
        )

    def integer(
        self,
        name: str,
        default: int = None,
        null: bool = False,
        index: bool = False,
        unique: bool = False,
        primary_key: bool = False,
        autoincrement: bool = False,
    ):
        self.add_column(
            column_name=name,
            column_type=types.Integer(autoincrement=autoincrement),
            default=default,
            null=null,
            index=index,
            unique=unique,
            primary_key=primary_key,
        )

    def small_integer(
        self,
        name: str,
        default: int = None,
        null: bool = False,
        index: bool = False,
        unique: bool = False,
        primary_key: bool = False,
        autoincrement: bool = False,
    ):
        self.add_column(
            column_name=name,
            column_type=types.SmallInteger(autoincrement=autoincrement),
            default=default,
            null=null,
            index=index,
            unique=unique,
            primary_key=primary_key,
        )

    def boolean(
        self,
        name: str,
        default: bool = None,
        null: bool = False,
        index: bool = False,
        unique: bool = False,
        primary_key: bool = False,
    ):
        self.add_column(
            column_name=name,
            column_type=types.Boolean(),
            default=default,
            null=null,
            index=index,
            unique=unique,
            primary_key=primary_key,
        )

    def date(
        self,
        name: str,
        default: datetime.date = None,
        null: bool = False,
        index: bool = False,
        unique: bool = False,
        primary_key: bool = False,
    ):
        self.add_column(
            column_name=name,
            column_type=types.Date(),
            default=default,
            null=null,
            index=index,
            unique=unique,
            primary_key=primary_key,
        )

    def time(
        self,
        name: str,
        default: datetime.time = None,
        null: bool = False,
        index: bool = False,
        unique: bool = False,
        primary_key: bool = False,
    ):
        self.add_column(
            column_name=name,
            column_type=types.Time(),
            default=default,
            null=null,
            index=index,
            unique=unique,
            primary_key=primary_key,
        )

    def timestamp(
        self,
        name: str,
        default: int = None,
        null: bool = False,
        index: bool = False,
        unique: bool = False,
        primary_key: bool = False,
    ):
        self.add_column(
            column_name=name,
            column_type=types.Timestamp(),
            default=default,
            null=null,
            index=index,
            unique=unique,
            primary_key=primary_key,
        )

    def interval(
        self,
        name: str,
        default: datetime.timedelta = None,
        null: bool = False,
        index: bool = False,
        unique: bool = False,
        primary_key: bool = False,
    ):
        self.add_column(
            column_name=name,
            column_type=types.Interval(),
            default=default,
            null=null,
            index=index,
            unique=unique,
            primary_key=primary_key,
        )

    def datetime(
        self,
        name: str,
        timezone: bool = False,
        default: datetime.datetime = None,
        null: bool = False,
        index: bool = False,
        unique: bool = False,
        primary_key: bool = False,
    ):
        self.add_column(
            column_name=name,
            column_type=types.DateTime(timezone),
            default=default,
            null=null,
            index=index,
            unique=unique,
            primary_key=primary_key,
        )

    def float(
        self,
        name: str,
        default: float = None,
        precision: int = None,
        scale: int = None,
        null: bool = False,
        index: bool = False,
        unique: bool = False,
        primary_key: bool = False,
    ):
        self.add_column(
            column_name=name,
            column_type=types.Float(precision=precision, scale=scale),
            default=default,
            null=null,
            index=index,
            unique=unique,
            primary_key=primary_key,
        )

    def decimal(
        self,
        name: str,
        default: decimallib.Decimal = None,
        precision: int = None,
        scale: int = None,
        null: bool = False,
        index: bool = False,
        unique: bool = False,
        primary_key: bool = False,
    ):
        self.add_column(
            column_name=name,
            column_type=types.Decimal(precision=precision, scale=scale),
            default=default,
            null=null,
            index=index,
            unique=unique,
            primary_key=primary_key,
        )

    numeric = float

    def binary(
        self,
        name: str,
        default: types.Binary = None,
        null: bool = False,
        index: bool = False,
        unique: bool = False,
        primary_key: bool = False,
    ):
        self.add_column(
            column_name=name,
            column_type=types.Binary(),
            default=default,
            null=null,
            index=index,
            unique=unique,
            primary_key=primary_key,
        )

    def json(
        self,
        name: str,
        default: t.Any = None,
        null: bool = False,
        index: bool = False,
        unique: bool = False,
        primary_key: bool = False,
    ):
        self.add_column(
            column_name=name,
            column_type=types.JSON(),
            default=default,
            null=null,
            index=index,
            unique=unique,
            primary_key=primary_key,
        )

    def uuid(
        self,
        name: str,
        default: uuidlib.UUID = None,
        null: bool = False,
        index: bool = False,
        unique: bool = False,
        primary_key: bool = False,
    ):
        self.add_column(
            column_name=name,
            column_type=types.UUID(),
            default=default,
            null=null,
            index=index,
            unique=unique,
            primary_key=primary_key,
        )

    def enum(
        self,
        name: str,
        values: t.Iterable,
        default: t.Any = None,
        null: bool = False,
        index: bool = False,
        unique: bool = False,
        primary_key: bool = False,
    ):
        self.add_column(
            column_name=name,
            column_type=types.Enum(list(values)),
            default=default,
            null=null,
            index=index,
            unique=unique,
            primary_key=primary_key,
        )

    def ip_address(
        self,
        name: str,
        default: str = None,
        null: bool = False,
        index: bool = False,
        unique: bool = False,
        primary_key: bool = False,
    ):
        self.add_column(
            column_name=name,
            column_type=types.IPAddress(),
            default=default,
            null=null,
            index=index,
            unique=unique,
            primary_key=primary_key,
        )

    def mac_address(
        self,
        name: str,
        default: str = None,
        null: bool = False,
        index: bool = False,
        unique: bool = False,
        primary_key: bool = False,
    ):
        self.add_column(
            column_name=name,
            column_type=types.MACAddress(),
            default=default,
            null=null,
            index=index,
            unique=unique,
            primary_key=primary_key,
        )

    def array(
        self,
        name: str,
        child_type: types.BaseType,
        default: t.Iterable = None,
        null: bool = False,
        index: bool = False,
        unique: bool = False,
        primary_key: bool = False,
    ):
        self.add_column(
            column_name=name,
            column_type=types.Array(child_type=child_type),
            default=default,
            null=null,
            index=index,
            unique=unique,
            primary_key=primary_key,
        )


class CreateTableBuilder(_AddsIndices, _AddColumns):
    def __init__(
        self,
        table_name: str,
        primary_key: t.Union[str, t.Iterable[str]] = None,
        temporary: bool = False,
        if_not_exists: bool = False,
        options: str = None,
    ) -> None:
        """
        create_table(:suppliers, options: 'ENGINE=InnoDB DEFAULT CHARSET=utf8mb4')

        Change the primary key column type
        create_table(:tags, id: :string) do |t|
          t.column :label, :string
        end

        Create a composite primary key
        create_table(:orders, primary_key: [:product_id, :client_id]) do |t|
          t.belongs_to :product
          t.belongs_to :client
        end

        create_table(:long_query, temporary: true,
          as: "SELECT * FROM orders INNER JOIN line_items ON order_id=orders.id")
        CREATE TEMPORARY TABLE long_query AS
          SELECT * FROM orders INNER JOIN line_items ON order_id=orders.id
        """
        self.table_name = table_name
        self.primary_key = []
        self.temporary = temporary
        self.if_not_exists = if_not_exists
        self.options = options
        self.as_select = None
        self._columns = []
        self._ops = []

        if primary_key is not None:
            if isinstance(primary_key, str):
                primary_key = [primary_key]
            self.primary_key = primary_key

    def add_column(
        self,
        column_name: str,
        column_type: types.BaseType,
        default: t.Union[int, float, str] = None,
        null: bool = False,
        index: bool = False,
        unique: bool = False,
        primary_key: bool = False,
    ) -> None:
        """Add column to a table.
        The type parameter is normally one of the migrations native types, which is one of the following:
        :primary_key, :string, :text, :integer, :bigint, :float, :decimal, :numeric, :datetime, :time,
        :date, :binary, :boolean.

        Available options are (none of these exists by default):
        :limit - Requests a maximum column length. This is the number of characters for a :string column and number of bytes for :text, :binary, and :integer columns. This option is ignored by some backends.
        :default - The column's default value. Use nil for NULL.
        :null - Allows or disallows NULL values in the column.
        :precision - Specifies the precision for the :decimal, :numeric, :datetime, and :time columns.
        :scale - Specifies the scale for the :decimal and :numeric columns.
        :collation - Specifies the collation for a :string or :text column. If not specified, the column will have the same collation as the table.
        :comment - Specifies the comment for the column. This option is ignored by some backends.
        """

        if index or unique:
            self.add_index(column_name, unique, if_not_exists=True)

        if primary_key:
            self.primary_key.append(column_name)

        self._columns.append(
            Column(
                name=column_name,
                type=column_type,
                default=default,
                null=null,
            )
        )

    def __iter__(self):
        return iter(
            [
                ops.CreateTable(
                    columns=self._columns,
                    table_name=self.table_name,
                    temporary=self.temporary,
                    if_not_exist=self.if_not_exists,
                    options_sql=self.options,
                    primary_key=self.primary_key,
                ),
                *self._ops,
            ]
        )


class AlterTableBuilder(_AddsIndices, _AddColumns):
    _ops: t.List[ops.Operation]

    def __init__(self, table: str):
        self.table_name = table
        self._ops = []

    def add_column(
        self,
        column_name: str,
        column_type: types.BaseType,
        default: t.Union[int, float, str] = None,
        null: bool = False,
        index: bool = False,
        unique: bool = False,
        primary_key: bool = False,
    ) -> None:
        self._ops.append(
            ops.AddColumn(
                table_name=self.table_name,
                column_name=column_name,
                column_type=column_type,
                default=default,
                null=null,
            )
        )
        if index or unique:
            self.add_index(column_name, unique, if_not_exists=True)

    def rename_column(
        self,
        column: str,
        new_name: str,
        if_exists: bool = False,
    ) -> None:
        self._ops.append(
            ops.RenameColumn(
                table_name=self.table_name,
                column_name=column,
                new_column_name=new_name,
                if_exists=if_exists,
            )
        )

    def drop_column(
        self,
        column: str,
        if_exists: bool = False,
        cascade: bool = False,
    ) -> None:
        """Drop column from a table."""
        self._ops.append(
            ops.DropColumn(
                table_name=self.table_name,
                column_name=column,
                if_exists=if_exists,
                cascade=cascade,
            )
        )

    def __iter__(self):
        return iter(self._ops)


class SchemaBuilder:
    def __init__(self, connection_factory: t.Callable[[], Connection]):
        self._connection = connection_factory()
        self._ops = []

    @property
    def grammar(self) -> BaseGrammar:
        return self._connection.driver.get_grammar()

    def add_operation(self, op: ops.Operation):
        self._ops.append(op)

    @contextmanager
    def create_table(
        self,
        table_name: str,
        primary_key: t.Union[str, t.Union[str]] = None,
        temporary: bool = False,
        if_not_exists: bool = False,
        options: str = None,
    ) -> t.ContextManager[CreateTableBuilder]:
        if primary_key is None:
            primary_key = []
        if isinstance(primary_key, str):
            primary_key = [primary_key]

        builder = CreateTableBuilder(
            table_name=table_name,
            primary_key=primary_key,
            temporary=temporary,
            if_not_exists=if_not_exists,
            options=options,
        )
        yield builder
        self._ops.extend(builder)

    @contextmanager
    def alter_table(self, table: str) -> AlterTableBuilder:
        builder = AlterTableBuilder()
        yield builder
        self._ops.extend(builder)

    def rename_table(
        self,
        table_name: str,
        new_table_name: str,
    ) -> None:
        self._ops.append(
            ops.RenameTable(
                table_name=table_name,
                new_table_name=new_table_name,
            )
        )

    def drop_table(self, table_name: str, if_exists: bool = False) -> None:
        self._ops.append(
            ops.DropTable(
                table_name=table_name,
                if_exists=if_exists,
            )
        )

    def drop_table_if_exists(self, table_name: str) -> None:
        self.drop_table(table_name, if_exists=True)

    def add_index(
        self,
        table_name: str,
        columns: t.Union[str, t.Iterable[str]],
        unique: bool = False,
        sort: t.Dict[str, str] = None,
        where: str = None,
        using: str = None,
        operator_class: str = None,
        name: str = None,
        if_not_exists: bool = None,
    ):
        self._ops.append(
            ops.AddIndex(
                table_name=table_name,
                columns=columns,
                name=name,
                sort=sort,
                where=where,
                operator_class=operator_class,
                unique=unique,
                using=using,
                if_not_exists=if_not_exists,
            )
        )

    def drop_index(
        self,
        name: str = None,
        if_exists: bool = False,
        cascade: bool = False,
        concurrently: bool = False,
    ) -> None:
        """Drop index."""
        self._ops.append(
            ops.DropIndex(
                index_name=name,
                if_exists=if_exists,
                cascade=cascade,
                concurrently=concurrently,
            )
        )

    def create_sequence(
        self,
        name: str,
        temporary: bool = False,
        if_not_exists: bool = False,
        increment_by: int = None,
        min_value: int = None,
        max_value: int = None,
        start_with: int = None,
        cycle: bool = False,
    ):
        self._ops.append(
            ops.CreateSequence(
                name=name,
                temporary=temporary,
                if_not_exists=if_not_exists,
                increment_by=increment_by,
                min_value=min_value,
                max_value=max_value,
                start_with=start_with,
                cycle=cycle,
            )
        )

    def alter_sequence(
        self,
        name: str,
        increment_by: int = None,
        min_value: int = None,
        max_value: int = None,
        start_with: int = None,
        restart_with: int = None,
        cycle: bool = None,
    ):
        self._ops.append(
            ops.AlterSequence(
                name=name,
                increment_by=increment_by,
                min_value=min_value,
                max_value=max_value,
                start_with=start_with,
                restart_with=restart_with,
                cycle=cycle,
            )
        )

    def rename_sequence(
        self,
        name: str,
        new_name: str,
    ) -> None:
        self._ops.append(ops.RenameSequence(name=name, new_name=new_name))

    def drop_sequence(
        self,
        name: str,
        if_exists: bool = False,
        cascade: bool = False,
    ):
        self._ops.append(
            ops.DropSequence(
                name=name,
                if_exists=if_exists,
                cascade=cascade,
            )
        )

    def create_view(
        self,
        name: str,
        sql: StrLike,
        replace: bool = False,
        temporary: bool = False,
        columns: t.Iterable[str] = None,
    ):
        self._ops.append(
            ops.CreateView(
                name=name,
                sql=sql,
                replace=replace,
                temporary=temporary,
                columns=columns,
            )
        )

    def rename_view(
        self,
        name: str,
        new_name: str,
        if_exists: bool,
    ) -> None:
        self._ops.append(
            ops.RenameView(
                name=name,
                new_name=new_name,
                if_exists=if_exists,
            )
        )

    def drop_view(
        self,
        name: str,
        if_exists: bool = False,
        cascade: bool = False,
    ) -> None:
        self._ops.append(
            ops.DropView(
                name=name,
                if_exists=if_exists,
                cascade=cascade,
            )
        )

    def raw(self, sql: str) -> None:
        self._ops.append(ops.RunSQL(sql))

    def clear(self):
        self._ops = []

    async def list_databases(self) -> t.List[str]:
        sql = self.grammar.get_list_databases_sql()
        async with self._connection as conn:
            return [row["name"] for row in await conn.fetch_all(sql)]

    async def has_database(self, name: str) -> bool:
        return name in await self.list_databases()

    async def list_sequences(
        self,
        schema: str = "public",
    ) -> t.List[Sequence]:
        sql = self.grammar.get_list_sequences_sql(schema)
        async with self._connection as conn:
            return [Sequence(**row) for row in await conn.fetch_all(sql)]

    async def has_sequence(self, name: str, schema: str = "public") -> bool:
        return name in [s.name for s in await self.list_sequences(schema)]

    async def list_views(self, schema: str = "public") -> t.List[View]:
        sql = self.grammar.get_list_views_sql(schema)
        async with self._connection as conn:
            return [View(**row) for row in await conn.fetch_all(sql)]

    async def has_view(self, name: str, schema: str = "public") -> bool:
        return name in [v.name for v in await self.list_views(schema)]

    async def list_tables(self, schema: str = "public") -> t.List[Table]:
        sql = self.grammar.get_list_tables_sql(schema)
        async with self._connection as conn:
            return [Table(**row) for row in await conn.fetch_all(sql)]

    async def has_table(self, name: str, schema: str = "public") -> bool:
        return name in [t.name for t in await self.list_tables(schema)]

    async def list_columns(
        self,
        table: str,
        schema: str = "public",
    ) -> t.List[Column]:
        pass

    async def has_column(self, name: str, table: str, schema: str = "public") -> bool:
        return name in [c.name for c in await self.list_columns(table, schema)]

    async def apply(self):
        async with self._connection as conn:
            for op in self:
                if inspect.iscoroutinefunction(op.get_sql):
                    sql = await op.get_sql()
                else:
                    sql = op.get_sql(conn.driver)
                print(sql)
                await conn.execute(sql)
        self.clear()

    def __iter__(self):
        return iter(self._ops)
