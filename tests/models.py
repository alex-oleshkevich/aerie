"""Neutral fixture models.

The suite was ported from an application whose tests leaned on its own domain
models. These two stand in for that shape: `Post` replaces the entity under test,
`Author` the related entity a join reaches, and `blog_id` the grouping key that
splits the fixtures into "mine" and "somebody else's".
"""

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column, relationship

from aerie.columns import IntPk
from aerie.models import Base, WithTimestamps


class Author(Base):
    __tablename__ = "test_authors"
    __repr_attrs__ = ["name"]

    id: Mapped[IntPk]
    # Nullable and declared first, so a shape's presence sentinel cannot rely on it.
    nickname: Mapped[str | None] = mapped_column(nullable=True, default=None)
    name: Mapped[str]
    blog_id: Mapped[int]

    posts: Mapped[list[Post]] = relationship(back_populates="author")


class Post(WithTimestamps, Base):
    __tablename__ = "test_posts"
    __repr_attrs__ = ["title"]

    id: Mapped[IntPk]
    title: Mapped[str]
    blog_id: Mapped[int]
    author_id: Mapped[int] = mapped_column(sa.ForeignKey("test_authors.id"))

    author: Mapped[Author] = relationship(back_populates="posts")


class Tag(WithTimestamps, Base):
    """Attribute names deliberately differ from column names.

    `excluded` and the conflict index are keyed by column name, so this model is
    what catches an upsert that confuses the two.
    """

    __tablename__ = "test_tags"
    __repr_attrs__ = ["slug"]

    id: Mapped[IntPk]
    slug: Mapped[str] = mapped_column("tag_slug", unique=True)
    label: Mapped[str] = mapped_column("tag_label")


class Counter(Base):
    """Carries a scalar `onupdate`, unlike the callable one on `WithTimestamps`."""

    __tablename__ = "test_counters"

    id: Mapped[IntPk]
    slug: Mapped[str] = mapped_column(unique=True)
    label: Mapped[str]
    revision: Mapped[int] = mapped_column(default=1, onupdate=99)


class Draft(Base):
    """Has a nullable relationship, so a shape can hit the missing-relation path."""

    __tablename__ = "test_drafts"

    id: Mapped[IntPk]
    title: Mapped[str]
    author_id: Mapped[int | None] = mapped_column(sa.ForeignKey("test_authors.id"), nullable=True)

    author: Mapped[Author | None] = relationship()
