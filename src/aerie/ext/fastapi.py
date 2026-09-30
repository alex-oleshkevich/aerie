import typing

import fastapi
from sqlalchemy.ext.asyncio import AsyncSession


def get_dbsession(request: fastapi.Request) -> AsyncSession:
    """Retrive SQLAlchemy session from the request. The session is managed by DbSessionMiddleware."""
    return typing.cast(AsyncSession, request.state.dbsession)


DbSession = typing.Annotated[AsyncSession, fastapi.Depends(get_dbsession)]
