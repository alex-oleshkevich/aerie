import typing as t
import sqlalchemy as sa
from sqlalchemy.orm import Mapped, relationship

from aerie import Model

metadata = sa.MetaData()
users_table = sa.Table(
    'users', metadata,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('name', sa.String, nullable=True),
)
profile_table = sa.Table(
    'profiles', metadata,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('first_name', sa.String),
    sa.Column('last_name', sa.String),
    sa.Column('user_id', sa.ForeignKey('users.id')),
)
address_table = sa.Table(
    'addresses', metadata,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('city', sa.String),
    sa.Column('street', sa.String),
)
user_to_address = sa.Table(
    'user_to_address', metadata,
    sa.Column('user_id', sa.ForeignKey(users_table.c.id), primary_key=True),
    sa.Column('address_id', sa.ForeignKey(address_table.c.id), primary_key=True),
)


class User(Model):
    __tablename__ = 'users'
    id = sa.Column(sa.BigInteger, primary_key=True)
    name = sa.Column(sa.String)
    profile: Mapped[t.Optional['Profile']] = relationship("Profile", back_populates="user", uselist=False)
    addresses: Mapped[list['Address']] = relationship("UserToAddress")


class Profile(Model):
    __tablename__ = 'profiles'
    id = sa.Column(sa.Integer, primary_key=True)
    first_name = sa.Column(sa.String)
    last_name = sa.Column(sa.String)
    user_id: Mapped[int] = sa.Column(sa.ForeignKey('users.id'))
    user: Mapped[t.Optional[User]] = relationship("User", back_populates="profile")


class Address(Model):
    __tablename__ = 'addresses'
    id = sa.Column(sa.Integer, primary_key=True)
    city = sa.Column(sa.String)
    street = sa.Column(sa.String)
    users: Mapped[list[User]] = relationship("UserToAddress")


class UserToAddress(Model):
    __tablename__ = 'user_to_address'
    user_id: Mapped[int] = sa.Column(sa.ForeignKey('users.id'), primary_key=True)
    address_id: Mapped[int] = sa.Column(sa.ForeignKey('addresses.id'), primary_key=True)
