from unittest.mock import Mock

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, load_only, mapped_column

from aerie.models import Base, import_model
from tests.conftest import OpenQuery
from tests.models import Post


def test_base_names_constraints_like_postgresql() -> None:
    table = sa.Table(
        "test_constraint_names",
        Base.metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("shop_id", sa.Integer),
        sa.Column("member_id", sa.Integer),
        sa.UniqueConstraint("shop_id", "member_id"),
        sa.ForeignKeyConstraint(["shop_id", "member_id"], ["shop_members.shop_id", "shop_members.user_id"]),
        sa.CheckConstraint("member_id > 0", name="member_id_positive"),
    )

    names = {type(constraint): constraint.name for constraint in table.constraints}
    assert names[sa.PrimaryKeyConstraint] == "test_constraint_names_pkey"
    assert names[sa.UniqueConstraint] == "test_constraint_names_shop_id_member_id_key"
    assert names[sa.ForeignKeyConstraint] == "test_constraint_names_shop_id_member_id_fkey"
    assert names[sa.CheckConstraint] == "member_id_positive"


class TestReprMixin:
    def test_no_identity(self) -> None:
        class NoIdentity(Base):
            __tablename__ = "no_identity"
            id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
            name: Mapped[str]

        instance = NoIdentity(name="test")
        assert repr(instance) == "NoIdentity(pk=None)"

    def test_single_pk(self) -> None:
        class SinglePk(Base):
            __tablename__ = "single_pk"
            id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
            name: Mapped[str]

        instance = SinglePk(id=42, name="test")
        mock_state = Mock()
        mock_state.identity = (42,)
        mock_state.unloaded = set()
        instance.__dict__["_sa_instance_state"] = mock_state
        assert repr(instance) == "SinglePk(pk=[42])"

    def test_composite_pk(self) -> None:
        class CompositePk(Base):
            __tablename__ = "composite_pk"
            id1: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
            id2: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
            name: Mapped[str]

        instance = CompositePk(id1=1, id2=2, name="test")
        mock_state = Mock()
        mock_state.identity = (1, 2)
        mock_state.unloaded = set()
        instance.__dict__["_sa_instance_state"] = mock_state
        assert repr(instance) == "CompositePk(pk=[1, 2])"

    def test_single_attr(self) -> None:
        class SingleAttr(Base):
            __tablename__ = "single_attr"
            __repr_attrs__ = ["name"]
            id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
            name: Mapped[str]

        instance = SingleAttr(id=1, name="test")
        mock_state = Mock()
        mock_state.identity = (1,)
        mock_state.unloaded = set()
        instance.__dict__["_sa_instance_state"] = mock_state
        assert repr(instance) == "SingleAttr(pk=[1] name='test')"

    def test_string_truncation(self) -> None:
        class StringTruncation(Base):
            __tablename__ = "truncation"
            __repr_attrs__ = ["name"]
            id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
            name: Mapped[str]

        instance = StringTruncation(id=1, name="this is a very long string that should be truncated")
        mock_state = Mock()
        mock_state.identity = (1,)
        mock_state.unloaded = set()
        instance.__dict__["_sa_instance_state"] = mock_state
        assert repr(instance) == "StringTruncation(pk=[1] name='this is a very ...')"

    def test_string_quoting(self) -> None:
        class StringQuoting(Base):
            __tablename__ = "quoting"
            __repr_attrs__ = ["name"]
            id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
            name: Mapped[str]

        instance = StringQuoting(id=1, name="short")
        mock_state = Mock()
        mock_state.identity = (1,)
        mock_state.unloaded = set()
        instance.__dict__["_sa_instance_state"] = mock_state
        result = repr(instance)
        assert result == "StringQuoting(pk=[1] name='short')"
        assert result.count("'") == 2

    def test_non_string(self) -> None:
        class NonString(Base):
            __tablename__ = "non_string"
            __repr_attrs__ = ["count", "active"]
            id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
            count: Mapped[int]
            active: Mapped[bool]

        instance = NonString(id=1, count=42, active=True)
        mock_state = Mock()
        mock_state.identity = (1,)
        mock_state.unloaded = set()
        instance.__dict__["_sa_instance_state"] = mock_state
        assert repr(instance) == "NonString(pk=[1] count=42 active=True)"

    def test_invalid_attr(self) -> None:
        class InvalidAttr(Base):
            __tablename__ = "invalid_attr"
            __repr_attrs__ = ["nonexistent"]
            id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)

        instance = InvalidAttr()
        with pytest.raises(KeyError, match="has incorrect attribute 'nonexistent'"):
            repr(instance)

    def test_custom_max_length(self) -> None:
        class CustomMaxLength(Base):
            __tablename__ = "custom_length"
            __repr_attrs__ = ["name"]
            __repr_max_length__ = 5
            id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
            name: Mapped[str]

        instance = CustomMaxLength(id=1, name="long string")
        mock_state = Mock()
        mock_state.identity = (1,)
        mock_state.unloaded = set()
        instance.__dict__["_sa_instance_state"] = mock_state
        assert repr(instance) == "CustomMaxLength(pk=[1] name='long ...')"


class TestBasePopulate:
    def test_populate_sets_attributes(self) -> None:
        class Populatable(Base):
            __tablename__ = "populatable"
            id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
            name: Mapped[str]

        instance = Populatable(id=1, name="before")
        instance.populate({"name": "after"})
        assert instance.name == "after"

    def test_populate_excludes_given_keys(self) -> None:
        class PopulatableExclude(Base):
            __tablename__ = "populatable_exclude"
            id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
            name: Mapped[str]

        instance = PopulatableExclude(id=1, name="before")
        instance.populate({"id": 2, "name": "after"}, exclude=["id"])
        assert instance.id == 1
        assert instance.name == "after"

    def test_populate_raises_for_unknown_attribute(self) -> None:
        class PopulatableUnknown(Base):
            __tablename__ = "populatable_unknown"
            id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)

        instance = PopulatableUnknown(id=1)
        with pytest.raises(AttributeError, match="Attribute nonexistent does not exist"):
            instance.populate({"nonexistent": "value"})


class TestImportModel:
    def test_imports_a_model_by_spec(self) -> None:
        assert import_model("tests.models:Post") is Post

    def test_rejects_a_spec_without_a_separator(self) -> None:
        with pytest.raises(ValueError, match="module.path:ClassName"):
            import_model("tests.models")

    def test_rejects_a_spec_without_a_class_name(self) -> None:
        with pytest.raises(ValueError, match="module.path:ClassName"):
            import_model("tests.models:")


class TestReprWithoutInstanceState:
    def test_uninspectable_instance_renders_a_marker(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import aerie.models as models_module

        instance = Post(title="x", blog_id=1, author_id=1)
        monkeypatch.setattr(models_module.sa, "inspect", lambda _obj: None)

        assert "<invalid-instance>" in repr(instance)


class TestReprDoesNotTriggerLoads:
    async def test_repr_of_expired_instance_reports_unloaded(
        self, dbsession: AsyncSession, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        alpha, _, _ = seeded_posts
        dbsession.expire(alpha)

        rendered = repr(alpha)

        assert "<not loaded>" in rendered
        assert "Post(" in rendered

    async def test_repr_of_loaded_instance_still_shows_values(self, seeded_posts: tuple[Post, Post, Post]) -> None:
        alpha, _, _ = seeded_posts
        assert "title='alpha-post'" in repr(alpha)


class TestBasePopulateWithUnloadedAttributes:
    async def test_setting_an_unloaded_attribute_works(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post], dbsession: AsyncSession
    ) -> None:
        dbsession.expunge_all()
        post = await query(Post).order_by(Post.title).options(load_only(Post.id)).first()
        assert post is not None

        post.populate({"title": "renamed"})

        assert post.title == "renamed"

    async def test_unknown_attribute_still_raises(
        self, query: OpenQuery, seeded_posts: tuple[Post, Post, Post]
    ) -> None:
        post = await query(Post).first()
        assert post is not None

        with pytest.raises(AttributeError, match="does not exist"):
            post.populate({"nonexistent": 1})
