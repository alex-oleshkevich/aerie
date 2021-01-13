import datetime
import importlib
import os
import pkgutil
import time
import typing as t
from dataclasses import dataclass
from types import ModuleType

import pypika as pk
import pypika.functions as fn

from aerie.database import Database
from aerie.protocols import OutputWriter
from aerie.schema import types


@dataclass
class Migration:
    name: str
    package: str
    path: str
    module: ModuleType

    @property
    def qual_name(self):
        return f"{self.package}.{self.name}"


class MigrationsRepository:
    def __init__(self, db: Database, table_name: str):
        self.db = db
        self.table_name = table_name

    async def get_completed(self) -> t.List[str]:
        sql = (
            pk.Query()
            .from_(self.table_name)
            .select("migration")
            .orderby(
                "batch",
                order=pk.Order.desc,
            )
        )
        rows = await self.db.fetch_all(sql)
        return rows.pluck("migration").as_list()

    async def ensure_table_exists(self) -> None:
        editor = self.db.schema_editor()
        with editor.create_table(self.table_name, if_not_exists=True) as table:
            table.add_column(
                "migration",
                types.String(512),
                primary_key=True,
                unique=True,
            )
            table.add_column("batch", types.Integer())
            table.add_column("ran_at", types.DateTime())
            table.add_index(
                ["migration", "batch"],
                unique=True,
                if_not_exists=True,
            )
        await editor.apply()

    async def get_next_batch(self) -> int:
        sql = pk.Query().from_(self.table_name).select(fn.Max(pk.Column("batch")) + 1)
        return (await self.db.fetch_val(sql)) or 1

    async def get_last_batch(self) -> int:
        sql = pk.Query().from_(self.table_name).select(fn.Max(pk.Column("batch")))
        return (await self.db.fetch_val(sql)) or 0

    async def get_last(self) -> t.List[str]:
        last_batch = await self.get_last_batch()
        sql = (
            pk.Query()
            .from_(self.table_name)
            .select("migration")
            .where(pk.Table(self.table_name).batch == last_batch)
            .orderby("migration")
        )
        rows = await self.db.fetch_all(sql)
        return rows.pluck("migration").as_list()

    async def get_migrations(self, steps: int) -> t.List[str]:
        sql = (
            pk.Query()
            .from_(self.table_name)
            .select(
                "migration",
            )
            .orderby("ran_at", order=pk.Order.desc)
            .limit(steps)
        )
        rows = await self.db.fetch_all(sql)
        return rows.pluck("migration").as_list()

    async def add(self, migration: str, batch: int) -> None:
        sql = (
            pk.Query()
            .into(self.table_name)
            .insert(
                migration,
                batch,
                datetime.datetime.now(),
            )
        )
        await self.db.execute(sql)

    async def delete(self, migration: str) -> None:
        sql = (
            pk.Query()
            .from_(self.table_name)
            .delete()
            .where(
                pk.Table(self.table_name).migration == migration,
            )
        )
        await self.db.execute(sql)


class Migrator:
    upgrade_fn = "upgrade"
    downgrade_fn = "downgrade"

    def __init__(
        self,
        database: Database,
        table_name: str = "migrations",
        log_writer: OutputWriter = None,
    ):
        self.table_name = table_name
        self.db = database
        self.repo = MigrationsRepository(database, table_name)
        self.log_writer = log_writer

    async def run_up(
        self,
        packages: t.List[str],
        dry_run: bool = False,
        print_queries: bool = False,
    ) -> None:
        await self.repo.ensure_table_exists()
        migrations = sorted(
            list(self.find_migrations(packages)),
            key=lambda m: m.qual_name,
        )
        completed = await self.repo.get_completed()
        next_batch = await self.repo.get_next_batch()
        to_execute = [
            migration
            for migration in migrations
            if migration.qual_name not in completed
        ]

        if not len(to_execute):
            self.log("[info]Nothing to migrate.[/info]")
            return

        for migration in to_execute:
            async with self.db.transaction():
                start_time = time.time()
                self.log(f"[comment]Migrating:[/comment] {migration.qual_name}")
                await self.run_migration(
                    migration=migration,
                    action=self.upgrade_fn,
                    dry_run=dry_run,
                    print_queries=print_queries,
                )
                if not dry_run:
                    await self.repo.add(migration.qual_name, next_batch)
                total_time = time.time() - start_time
                self.log(
                    f"[info]Migrated:[/info] {migration.qual_name} ({total_time:.2f}ms)"
                )

    async def run_down(
        self,
        packages: t.List[str],
        steps: int = None,
        dry_run: bool = False,
        print_queries: bool = False,
    ) -> None:
        await self.repo.ensure_table_exists()
        if steps:
            ran_migrations = await self.repo.get_migrations(steps)
        else:
            ran_migrations = await self.repo.get_last()

        disk_migrations = {m.qual_name: m for m in self.find_migrations(packages)}

        to_rollback = []
        for migration in ran_migrations:
            if migration in disk_migrations:
                to_rollback.append(disk_migrations[migration])
            else:
                self.log(f"[error]Migration file missing:[/error] {migration}.")

        if not len(to_rollback):
            self.log("[info]Nothing to rollback.[/info]")
            return

        for migration in to_rollback:
            async with self.db.transaction():
                start_time = time.time()
                self.log(f"[comment]Rolling back:[/comment] {migration.qual_name}")
                await self.run_migration(
                    migration=migration,
                    action=self.downgrade_fn,
                    dry_run=dry_run,
                    print_queries=print_queries,
                )
                if not dry_run:
                    await self.repo.delete(migration.qual_name)
                total_time = time.time() - start_time
                self.log(
                    f"[info]Rolled back:[/info] {migration.qual_name} "
                    f"({total_time:.2f}ms)"
                )

    async def reset(
        self,
        packages: t.List[str],
        dry_run: bool = False,
        print_queries: bool = False,
    ) -> None:
        """Rollback all migrations."""
        await self.run_down(packages, 999_999, dry_run, print_queries)

    async def run_migration(
        self,
        migration: Migration,
        action: str,
        dry_run: bool = False,
        print_queries: bool = False,
    ):
        method = getattr(migration.module, action)
        changeset = self.db.schema_editor()
        method(changeset)
        for operation in changeset:
            sql = operation.get_sql(self.db.driver)
            if print_queries:
                self.log(sql)
            if not dry_run:
                await self.db.execute(sql)

    def find_migrations(
        self,
        packages: t.List[str],
    ) -> t.Iterable[Migration]:
        migrations = []
        for package in packages:
            loader = pkgutil.get_loader(package)
            if not loader:
                raise ValueError(f"Could not import migrations from {package}.")
            module_dir = os.path.dirname(loader.path)
            for module_info in pkgutil.iter_modules([module_dir]):
                module_instance = importlib.import_module(
                    f"{package}.{module_info.name}"
                )
                migrations.append(
                    Migration(
                        name=module_info.name,
                        package=package,
                        path=os.path.join(package, module_info.name),
                        module=module_instance,
                    )
                )
        return sorted(migrations, key=lambda m: m.qual_name)

    def log(self, message: str) -> None:
        if self.log_writer:
            self.log_writer.write(message)
