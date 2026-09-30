import datetime
import typing
import uuid

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY, INET, JSONB, TSTZRANGE, TSVECTOR, Range
from sqlalchemy.orm import mapped_column

T = typing.TypeVar("T")


IntPk = typing.Annotated[int, mapped_column(sa.Integer, primary_key=True, autoincrement=True)]
BigIntPk = typing.Annotated[int, mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)]

UUIDPk = typing.Annotated[uuid.UUID, mapped_column(sa.UUID(), primary_key=True, default=uuid.uuid7)]
RandomUUIDPk = typing.Annotated[uuid.UUID, mapped_column(sa.UUID(), primary_key=True, default=uuid.uuid4)]

ZeroInt = typing.Annotated[int, mapped_column(sa.Integer, default=0, server_default=sa.text("0"))]
EmptyText = typing.Annotated[str, mapped_column(sa.Text, default="", server_default=sa.text("''"))]
FalseBool = typing.Annotated[bool, mapped_column(sa.Boolean, default=False, server_default=sa.false())]

AwareDateTime = typing.Annotated[datetime.datetime, mapped_column(sa.DateTime(timezone=True))]
DateTime = typing.Annotated[datetime.datetime, mapped_column(sa.DateTime())]
Date = typing.Annotated[datetime.date, mapped_column(sa.Date)]
Duration = typing.Annotated[datetime.timedelta, mapped_column(sa.Interval)]

JSONDict = typing.Annotated[dict[str, typing.Any], mapped_column(JSONB, default=dict, server_default=sa.text("'{}'"))]
JSONList = typing.Annotated[list[T], mapped_column(JSONB, default=list, server_default=sa.text("'[]'"))]

TextArray = typing.Annotated[list[str], mapped_column(ARRAY(sa.Text), default=list, server_default=sa.text("'{}'"))]

Slug = typing.Annotated[str, mapped_column(sa.String(255), index=True)]
Email = typing.Annotated[str, mapped_column(sa.String(320))]
Url = typing.Annotated[str, mapped_column(sa.Text)]
IPAddress = typing.Annotated[str, mapped_column(INET)]
SearchVector = typing.Annotated[str, mapped_column(TSVECTOR)]
TimeRange = typing.Annotated[Range[datetime.datetime], mapped_column(TSTZRANGE)]

AutoCreatedAt = typing.Annotated[
    datetime.datetime,
    mapped_column(
        sa.DateTime(timezone=True),
        default=lambda: datetime.datetime.now(datetime.UTC),
        server_default=sa.func.now(),
    ),
]
AutoUpdatedAt = typing.Annotated[
    datetime.datetime,
    mapped_column(
        sa.DateTime(timezone=True),
        server_default=sa.func.now(),
        onupdate=lambda: datetime.datetime.now(datetime.UTC),
        default=lambda: datetime.datetime.now(datetime.UTC),
    ),
]

DeletedAt = typing.Annotated[
    datetime.datetime | None,
    mapped_column(sa.DateTime(timezone=True), nullable=True, default=None, index=True),
]
