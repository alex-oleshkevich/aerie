import enum
import importlib
import typing

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped

from aerie.columns import AutoCreatedAt, AutoUpdatedAt, UUIDPk


class WithTimestamps:
    __abstract__ = True
    created_at: Mapped[AutoCreatedAt]
    updated_at: Mapped[AutoUpdatedAt]


class WithUUIDKey:
    __abstract__ = True
    uuid: Mapped[UUIDPk]


class ReprMixin:
    __abstract__ = True

    __repr_attrs__: typing.ClassVar[list[str]] = []
    __repr_max_length__ = 15

    @property
    def _id_str(self) -> str:
        instance = sa.inspect(self)
        if not instance:
            return "<invalid-instance>"

        ids = instance.identity
        if ids:
            joined = ", ".join([str(x) for x in ids]) if len(ids) > 1 else str(ids[0])
            return f"[{joined}]"
        else:
            return "None"

    @property
    def _repr_attrs_str(self) -> str:
        max_length = self.__repr_max_length__
        state = sa.inspect(self)
        unloaded = state.unloaded if state else set()

        values = []
        for key in self.__repr_attrs__:
            # Ask the class, not the instance: `hasattr(self, key)` would itself
            # trigger the load this method is trying to avoid.
            if not hasattr(type(self), key):
                raise KeyError(f"{self.__class__} has incorrect attribute '{key}' in __repr__attrs__")

            # Never emit a lazy load from __repr__. On an AsyncSession that raises
            # MissingGreenlet, which would break logging and exception formatting.
            if key in unloaded:
                values.append(f"{key}=<not loaded>")
                continue

            value = getattr(self, key)
            wrap_in_quote = isinstance(value, str)

            value = str(value)
            if len(value) > max_length:
                value = value[:max_length] + "..."

            if wrap_in_quote:
                value = f"'{value}'"
            values.append(f"{key}={value}")

        return " ".join(values)

    def __repr__(self) -> str:
        # Each property walks the instance state, so read them once.
        id_str = self._id_str
        attrs_str = self._repr_attrs_str
        # `_id_str` always returns something, so there is no empty case to guard.
        suffix = f" {attrs_str}" if attrs_str else ""
        return f"{type(self).__name__}(pk={id_str}{suffix})"


class Base(ReprMixin, AsyncAttrs, DeclarativeBase):
    """Declarative base. Deliberately has no query methods -- reads start from `query(Model)`."""

    __abstract__ = True

    metadata = sa.MetaData(
        naming_convention={
            "pk": "%(table_name)s_pkey",
            "uq": "%(table_name)s_%(column_0_N_name)s_key",
            "fk": "%(table_name)s_%(column_0_N_name)s_fkey",
            "ck": "%(constraint_name)s",
            "ix": "ix_%(column_0_label)s",
        }
    )

    type_annotation_map: typing.ClassVar[dict[typing.Any, typing.Any]] = {
        enum.StrEnum: sa.Enum(enum.StrEnum, values_callable=lambda e: [m.value for m in e]),
    }

    def populate(self, values: dict[str, typing.Any], exclude: typing.Sequence[str] | None = None) -> None:
        for name, value in values.items():
            if exclude and name in exclude:
                continue
            # Ask the class: reading the instance would trigger the very load that
            # raiseload/expiry makes fatal on an AsyncSession.
            if not hasattr(type(self), name):
                raise AttributeError(f"Attribute {name} does not exist on model {self}.")
            setattr(self, name, value)


def import_model(spec: str) -> type[Base]:
    module_name, separator, class_name = spec.partition(":")
    if not separator or not class_name:
        raise ValueError(f"Model spec must be 'module.path:ClassName', got {spec!r}.")

    module = importlib.import_module(module_name)
    return typing.cast(type[Base], getattr(module, class_name))
