"""Framework integrations."""

import types

from sqlalchemy.ext.asyncio import AsyncSession

from aerie.ext.fastapi import DbSession, get_dbsession


class TestGetDbSession:
    def test_reads_the_session_the_middleware_put_on_the_request(self, dbsession: AsyncSession) -> None:
        request = types.SimpleNamespace(state=types.SimpleNamespace(dbsession=dbsession))

        assert get_dbsession(request) is dbsession  # type: ignore[arg-type]

    def test_dbsession_annotation_is_exported(self, dbsession: AsyncSession) -> None:
        assert DbSession is not None
