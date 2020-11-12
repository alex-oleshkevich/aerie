import typing as t
import urllib.parse
from urllib.parse import ParseResult

from pypika.queries import QueryBuilder

from aerie.expr import OnConflict, OnConflictHandler


class Adapter:
    name = None

    def insert(
            self,
            table: str,
            values: t.Dict,
            returning: t.Union[str, t.List[str]] = None,
            on_conflict: OnConflictHandler = None,
    ):
        raise NotImplementedError()

    def insert_all(
            self,
            table: str,
            values: t.List[t.Mapping],
            returning: t.Union[str, t.List[str]] = None,
            on_conflict: OnConflictHandler = None,
    ):
        raise NotImplementedError()

    def update(
            self,
            table: str,
            values: t.Dict,
            where=None,
            returning: t.Union[str, t.List[str]] = None,
    ):
        raise NotImplementedError()

    def delete(
            self,
            table: str,
            where=None,
    ):
        raise NotImplementedError()

    def insert_from_select(self): ...

    def build_on_conflict(self, query: QueryBuilder,
                           on_conflict: OnConflict) -> QueryBuilder:
        raise NotImplementedError()

    def build_returning(self, query: QueryBuilder,
                        returning: t.List[str]) -> QueryBuilder:
        raise NotImplementedError()

    @classmethod
    def create_from_url(cls, url: ParseResult):
        username = url.username
        password = url.password
        host = url.hostname
        port = url.port
        options = {
            pair[0]: pair[1]
            for pair in urllib.parse.parse_qsl(url.query)
        }
        db_name = url.path[0:]
        return cls(
            username=username, password=password, host=host, port=port,
            db_name=db_name,
            **options,
        )
